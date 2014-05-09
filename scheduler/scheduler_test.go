package scheduler_test

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/cloudfoundry-incubator/executor/client"

	"github.com/cloudfoundry-incubator/executor/client/fake_client"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gosteno"
	"github.com/onsi/gomega/ghttp"

	"github.com/cloudfoundry-incubator/rep/scheduler"

	. "github.com/onsi/ginkgo"
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
		var schedulerAddr string
		var gameScheduler *scheduler.Scheduler
		var correctStack = "my-stack"
		var fakeClient *fake_client.FakeClient

		BeforeEach(func() {
			schedulerAddr = fmt.Sprintf("127.0.0.1:%d", 12000+GinkgoParallelNode())
			fakeClient = fake_client.New()
			fakeExecutor = ghttp.NewServer()
			fakeBBS = fake_bbs.NewFakeExecutorBBS()

			gameScheduler = scheduler.New(fakeBBS, logger, correctStack, schedulerAddr, fakeClient)
		})

		AfterEach(func() {
			gameScheduler.Stop()
			fakeExecutor.Close()
		})

		JustBeforeEach(func() {
			readyChan := make(chan struct{})
			err := gameScheduler.Run(readyChan)
			Ω(err).ShouldNot(HaveOccurred())
			<-readyChan
		})

		Context("when a staging task is desired", func() {
			var task models.Task

			BeforeEach(func() {
				index := 0

				task = models.Task{
					Guid:       "task-guid-123",
					Stack:      correctStack,
					MemoryMB:   64,
					DiskMB:     1024,
					CpuPercent: .5,
					Actions: []models.ExecutorAction{
						{
							Action: models.RunAction{
								Script:  "the-script",
								Env:     []models.EnvironmentVariable{{Key: "PATH", Value: "the-path"}},
								Timeout: 500,
							},
						},
					},
					Log: models.LogConfig{
						Guid:       "some-guid",
						SourceName: "XYZ",
						Index:      &index,
					},
				}
				fakeBBS.EmitDesiredTask(task)
			})

			Context("when reserving the container succeeds", func() {
				var allocateCalled chan struct{}
				var deletedContainerGuid chan string

				BeforeEach(func() {
					allocateCalled = make(chan struct{}, 1)
					deletedContainerGuid = make(chan string, 1)

					fakeClient.WhenAllocatingContainer = func(req client.ContainerRequest) (client.ContainerResponse, error) {
						defer GinkgoRecover()

						allocateCalled <- struct{}{}
						Ω(fakeBBS.ClaimedTasks()).Should(HaveLen(0))
						Ω(req.MemoryMB).Should(Equal(64))
						Ω(req.DiskMB).Should(Equal(1024))
						Ω(req.CpuPercent).Should(Equal(0.5))
						Ω(req.LogConfig).Should(Equal(task.Log))
						return client.ContainerResponse{ExecutorGuid: "the-executor-guid", Guid: "guid-123", ContainerRequest: req}, nil
					}

					fakeClient.WhenDeletingContainer = func(allocationGuid string) error {
						deletedContainerGuid <- allocationGuid
						return nil
					}
				})

				Context("when claiming the task succeeds", func() {
					Context("when initializing the container succeeds", func() {
						var initCalled chan struct{}

						BeforeEach(func() {
							initCalled = make(chan struct{}, 1)

							fakeClient.WhenInitializingContainer = func(allocationGuid string) error {
								defer GinkgoRecover()

								initCalled <- struct{}{}
								Ω(allocationGuid).Should(Equal("guid-123"))
								Ω(fakeBBS.ClaimedTasks()).Should(HaveLen(1))
								Ω(fakeBBS.StartedTasks()).Should(HaveLen(0))
								return nil
							}
						})

						Context("and the executor successfully starts running the task", func() {
							var (
								reqChan chan client.RunRequest
							)

							BeforeEach(func() {
								reqChan = make(chan client.RunRequest, 1)

								fakeClient.WhenRunning = func(allocationGuid string, req client.RunRequest) error {
									defer GinkgoRecover()

									Ω(fakeBBS.StartedTasks()).Should(HaveLen(1))

									expectedTask := task
									expectedTask.ExecutorID = "the-executor-guid"
									expectedTask.ContainerHandle = "guid-123"
									Ω(fakeBBS.StartedTasks()[0]).Should(Equal(expectedTask))

									Ω(allocationGuid).Should(Equal("guid-123"))
									Ω(req.Actions).Should(Equal(task.Actions))

									reqChan <- req
									return nil
								}
							})

							It("makes all calls to the executor", func() {
								Eventually(allocateCalled).Should(Receive())
								Eventually(initCalled).Should(Receive())
								Eventually(reqChan).Should(Receive())
							})

							Describe("and the task succeeds", func() {
								var resp *http.Response
								var err error

								JustBeforeEach(func() {
									resp, err = sendCompletionCallback(reqChan, client.ContainerRunResult{
										Result: "42",
									})
								})

								It("responds to the onComplete hook", func() {
									Ω(resp.StatusCode).Should(Equal(http.StatusOK))
									Ω(err).ShouldNot(HaveOccurred())
								})

								It("records the job result", func() {
									Eventually(fakeBBS.CompletedTasks).Should(HaveLen(1))
									Ω(fakeBBS.CompletedTasks()[0].Guid).Should(Equal(task.Guid))
									Ω(fakeBBS.CompletedTasks()[0].Result).Should(Equal("42"))
								})
							})

							Describe("and the task fails", func() {
								var resp *http.Response
								var err error

								JustBeforeEach(func() {
									resp, err = sendCompletionCallback(reqChan, client.ContainerRunResult{
										Failed:        true,
										FailureReason: "it didn't work",
									})
								})

								It("responds to the onComplete hook", func() {
									Ω(err).ShouldNot(HaveOccurred())
									Ω(resp.StatusCode).Should(Equal(http.StatusOK))
								})

								It("records the job failure", func() {
									expectedTask := task
									expectedTask.ContainerHandle = "guid-123"
									expectedTask.ExecutorID = "the-executor-guid"
									expectedTask.Failed = true
									expectedTask.FailureReason = "it didn't work"

									Eventually(fakeBBS.CompletedTasks).Should(HaveLen(1))
									Ω(fakeBBS.CompletedTasks()[0]).Should(Equal(expectedTask))
								})
							})
						})

						Context("but starting the task fails", func() {
							BeforeEach(func() {
								fakeBBS.SetStartTaskErr(errors.New("kerpow"))
							})

							It("deletes the container", func() {
								Eventually(deletedContainerGuid).Should(Receive(Equal("guid-123")))
							})
						})
					})

					Context("but initializing the container fails", func() {
						BeforeEach(func() {
							fakeClient.WhenInitializingContainer = func(allocationGuid string) error {
								return errors.New("Can't initialize")
							}
						})

						It("does not mark the job as started", func() {
							Eventually(fakeBBS.StartedTasks).Should(HaveLen(0))
						})

						It("deletes the container", func() {
							Eventually(deletedContainerGuid).Should(Receive(Equal("guid-123")))
						})
					})
				})

				Context("but claiming the task fails", func() {
					BeforeEach(func() {
						fakeBBS.SetClaimTaskErr(errors.New("data store went away."))
					})

					It("deletes the resource allocation on the executor", func() {
						Eventually(deletedContainerGuid).Should(Receive(Equal("guid-123")))
					})
				})
			})

			Context("when reserving the container fails", func() {
				var allocatedContainer chan struct{}

				BeforeEach(func() {
					allocatedContainer = make(chan struct{}, 1)

					fakeClient.WhenAllocatingContainer = func(req client.ContainerRequest) (client.ContainerResponse, error) {
						allocatedContainer <- struct{}{}
						return client.ContainerResponse{}, errors.New("Something went wrong")
					}
				})

				It("makes the resource allocation request", func() {
					Eventually(allocatedContainer).Should(Receive())
				})

				It("does not mark the job as Claimed", func() {
					Eventually(fakeBBS.ClaimedTasks).Should(HaveLen(0))
				})

				It("does not mark the job as Started", func() {
					Eventually(fakeBBS.StartedTasks).Should(HaveLen(0))
				})
			})
		})

		Context("when the task has the wrong stack", func() {
			var task models.Task

			BeforeEach(func() {
				task = models.Task{
					Guid:       "task-guid-123",
					Stack:      "asd;oubhasdfbuvasfb",
					MemoryMB:   64,
					DiskMB:     1024,
					CpuPercent: .5,
					Actions:    []models.ExecutorAction{},
				}
				fakeBBS.EmitDesiredTask(task)
			})

			It("ignores the task", func() {
				Consistently(fakeBBS.ClaimedTasks).Should(BeEmpty())
			})
		})
	})
})

func sendCompletionCallback(reqChan chan client.RunRequest, resp client.ContainerRunResult) (*http.Response, error) {
	var req client.RunRequest

	select {
	case req = <-reqChan:
	case <-time.After(2 * time.Second):
		Fail("request timeout exceeded")
	}

	resp.Metadata = req.Metadata

	body, jsonErr := fake_client.MarshalContainerRunResult(resp)
	Ω(jsonErr).ShouldNot(HaveOccurred())

	return http.Post(req.CompletionURL, "application/json", bytes.NewReader(body))
}
