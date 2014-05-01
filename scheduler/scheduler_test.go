package scheduler_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gosteno"
	"github.com/onsi/gomega/ghttp"

	"os"
	"syscall"

	"github.com/cloudfoundry-incubator/representative/scheduler"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
)

var _ = Describe("Scheduler", func() {
	var logger *gosteno.Logger

	BeforeSuite(func() {
		gosteno.EnterTestMode(gosteno.LOG_DEBUG)
	})

	BeforeEach(func() {
		logger = gosteno.NewLogger("test-logger")
	})

	Context("when a game scheduler is running", func() {
		var fakeExecutor *ghttp.Server
		var fakeBBS *fake_bbs.FakeExecutorBBS
		var schedulerAddr = fmt.Sprintf("127.0.0.1:%d", 12001+config.GinkgoConfig.ParallelNode)
		var sigChan chan os.Signal
		var gameScheduler *scheduler.Scheduler
		var schedulerEnded chan struct{}

		BeforeEach(func() {
			fakeExecutor = ghttp.NewServer()
			fakeBBS = fake_bbs.NewFakeExecutorBBS()
			sigChan = make(chan os.Signal, 1)

			gameScheduler = scheduler.New(fakeBBS, logger, schedulerAddr, fakeExecutor.URL())
		})

		AfterEach(func() {
			sigChan <- syscall.SIGINT
			<-schedulerEnded
			fakeExecutor.Close()
		})

		JustBeforeEach(func() {
			readyChan := make(chan struct{})
			schedulerEnded = make(chan struct{})
			go func() {
				err := gameScheduler.Run(sigChan, readyChan)
				Ω(err).ShouldNot(HaveOccurred())
				close(schedulerEnded)
			}()
			<-readyChan
		})

		Context("when a staging task is desired", func() {
			var task *models.Task

			BeforeEach(func() {
				task = &models.Task{
					Guid:            "task-guid-123",
					MemoryMB:        64,
					DiskMB:          1024,
					CpuPercent:      .5,
					FileDescriptors: 512,
					Actions:         []models.ExecutorAction{},
				}
				fakeBBS.EmitDesiredTask(task)
			})

			reserveContainerSuccessful := ghttp.CombineHandlers(
				func(w http.ResponseWriter, r *http.Request) {
					Ω(fakeBBS.ClaimedTasks()).Should(HaveLen(0))
				},
				ghttp.VerifyRequest("POST", "/resource_allocations"),
				ghttp.VerifyJSON(`{"memory_mb":64, "disk_mb":1024, "cpu_percent":0.5, "file_descriptors": 512}`),
				ghttp.RespondWith(http.StatusCreated, `{"executor_guid":"executor-guid","allocation_guid":"guid-123"}`))

			createContainerSuccessful := ghttp.CombineHandlers(
				func(w http.ResponseWriter, r *http.Request) {
					Ω(fakeBBS.ClaimedTasks()).Should(HaveLen(1))
					Ω(fakeBBS.StartedTasks()).Should(HaveLen(0))
				},
				ghttp.VerifyRequest("POST", "/resource_allocations/guid-123/container"),
				ghttp.RespondWith(http.StatusCreated, ""))

			startContainerSuccessful := ghttp.CombineHandlers(
				func(w http.ResponseWriter, r *http.Request) {
					Ω(fakeBBS.StartedTasks()).Should(HaveLen(1))
					startedTask := fakeBBS.StartedTasks()[0]

					jobRequest := models.JobRequest{
						Actions:       startedTask.Actions,
						Metadata:      startedTask.ToJSON(),
						CompletionURL: fmt.Sprintf("http://%s/complete/guid-123", schedulerAddr),
					}

					ghttp.VerifyJSONRepresenting(jobRequest)(w, r)
				},
				ghttp.VerifyRequest("POST", "/resource_allocations/guid-123/actions"),
				ghttp.RespondWith(http.StatusCreated, ""))

			deleteAllocationSuccessful := ghttp.CombineHandlers(
				ghttp.VerifyRequest("DELETE", "/resource_allocations/guid-123"),
				ghttp.RespondWith(http.StatusOK, ""))

			Context("and we reserve it, claim it, run it, and the task finishes", func() {
				BeforeEach(func() {
					fakeExecutor.AppendHandlers(
						reserveContainerSuccessful,
						createContainerSuccessful,
						startContainerSuccessful,
					)
				})

				It("completes all calls to the executor", func() {
					Eventually(fakeExecutor.ReceivedRequests).Should(HaveLen(3))
				})

				It("marks the job as Claimed", func() {
					Eventually(fakeBBS.ClaimedTasks).Should(HaveLen(1))
				})

				It("marks the job as Started", func() {
					Eventually(fakeBBS.StartedTasks).Should(HaveLen(1))
				})

				Describe("and the task succeeds", func() {
					var resp *http.Response
					var err error
					var expectedTask *models.Task

					JustBeforeEach(func() {
						expectedTask = &models.Task{
							Guid:            "task-guid-123",
							ExecutorID:      "executor-id",
							ContainerHandle: "guid-123",
						}

						body, jsonErr := json.Marshal(models.JobResponse{
							AllocationGuid: "guid-123",
							ExecutorGuid:   "executor-guid",
							Result:         "42",
							Metadata:       expectedTask.ToJSON(),
						})
						Ω(jsonErr).ShouldNot(HaveOccurred())

						resp, err = http.Post(fmt.Sprintf("http://%s/complete", schedulerAddr), "application/json", bytes.NewReader(body))
					})

					It("responds to the onComplete hook", func() {
						Ω(err).ShouldNot(HaveOccurred())
						Ω(resp.StatusCode).Should(Equal(http.StatusOK))
					})

					It("records the job result", func() {
						expectedTask.Result = "42"
						Eventually(fakeBBS.CompletedTasks).Should(HaveLen(1))
						Ω(fakeBBS.CompletedTasks()[0]).Should(Equal(expectedTask))
					})
				})

				Describe("and the task fails", func() {
					var resp *http.Response
					var err error
					var expectedTask *models.Task

					JustBeforeEach(func() {
						expectedTask = &models.Task{
							Guid:            "task-guid-123",
							ExecutorID:      "executor-id",
							ContainerHandle: "guid-123",
						}

						body, jsonErr := json.Marshal(models.JobResponse{
							AllocationGuid: "guid-123",
							ExecutorGuid:   "executor-guid",
							Failed:         true,
							FailureReason:  "it didn't work",
							Metadata:       expectedTask.ToJSON(),
						})
						Ω(jsonErr).ShouldNot(HaveOccurred())

						resp, err = http.Post(fmt.Sprintf("http://%s/complete", schedulerAddr), "application/json", bytes.NewReader(body))
					})

					It("responds to the onComplete hook", func() {
						Ω(err).ShouldNot(HaveOccurred())
						Ω(resp.StatusCode).Should(Equal(http.StatusOK))
					})

					It("records the job failure", func() {
						expectedTask.Failed = true
						expectedTask.FailureReason = "it didn't work"
						Eventually(fakeBBS.CompletedTasks).Should(HaveLen(1))
						Ω(fakeBBS.CompletedTasks()[0]).Should(Equal(expectedTask))
					})
				})
			})

			Context("and we can't reserve it", func() {
				var reserveContainerFailed = ghttp.CombineHandlers(
					func(w http.ResponseWriter, r *http.Request) {
						Ω(fakeBBS.ClaimedTasks()).Should(HaveLen(0))
					},
					ghttp.VerifyRequest("POST", "/resource_allocations"),
					ghttp.RespondWith(http.StatusRequestEntityTooLarge, `{"executor_guid":"executor-guid","allocation_guid":"guid-123"}`))

				BeforeEach(func() {
					fakeExecutor.AppendHandlers(
						reserveContainerFailed,
					)
				})

				It("completes the resource allocation request", func() {
					Eventually(fakeExecutor.ReceivedRequests).Should(HaveLen(1))
				})

				It("does not mark the job as Claimed", func() {
					Eventually(fakeBBS.ClaimedTasks).Should(HaveLen(0))
				})

				It("does not mark the job as Started", func() {
					Eventually(fakeBBS.StartedTasks).Should(HaveLen(0))
				})
			})

			Context("and we reserve it but can't claim it", func() {
				BeforeEach(func() {
					fakeExecutor.AppendHandlers(
						reserveContainerSuccessful,
						deleteAllocationSuccessful,
					)
					fakeBBS.SetClaimTaskErr(errors.New("data store went away."))
				})

				It("deletes the resource allocation on the executor", func() {
					Eventually(fakeExecutor.ReceivedRequests).Should(HaveLen(2))
				})
			})

			Context("and we reserve it, claim it, but can't create the container (resources go away between reserve and run)", func() {
				createContainerFailed := ghttp.CombineHandlers(
					func(w http.ResponseWriter, r *http.Request) {
						Ω(fakeBBS.ClaimedTasks()).Should(HaveLen(1))
						Ω(fakeBBS.StartedTasks()).Should(HaveLen(0))
					},
					ghttp.VerifyRequest("POST", "/resource_allocations/guid-123/container"),
					ghttp.RespondWith(http.StatusInternalServerError, ""))

				BeforeEach(func() {
					fakeExecutor.AppendHandlers(
						reserveContainerSuccessful,
						createContainerFailed,
						deleteAllocationSuccessful,
					)
				})

				It("does not mark the job as started", func() {
					Eventually(fakeBBS.StartedTasks).Should(HaveLen(0))
				})
			})

			Context("and we reserve it, claim it, create container, but can't mark it as started", func() {
				BeforeEach(func() {
					fakeBBS.SetStartTaskErr(errors.New("kerpow"))
					fakeExecutor.AppendHandlers(
						reserveContainerSuccessful,
						createContainerSuccessful,
						deleteAllocationSuccessful,
					)
				})

				It("deletes the allocation", func() {
					Eventually(fakeExecutor.ReceivedRequests).Should(HaveLen(3))
				})
			})
		})
	})

	Context("when task watcher throws an error", func() {})
})
