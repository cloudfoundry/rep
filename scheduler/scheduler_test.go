package scheduler_test

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/cloudfoundry-incubator/executor/client"
	"net/http"
	"reflect"

	"github.com/cloudfoundry-incubator/executor/client/fake_client"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gosteno"
	"github.com/onsi/gomega/ghttp"

	"os"
	"syscall"

	"github.com/cloudfoundry-incubator/rep/scheduler"

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
		var correctStack = "my-stack"
		var fakeClient *fake_client.FakeClient

		BeforeEach(func() {
			fakeClient = fake_client.New()
			fakeExecutor = ghttp.NewServer()
			fakeBBS = fake_bbs.NewFakeExecutorBBS()
			sigChan = make(chan os.Signal, 1)

			gameScheduler = scheduler.New(fakeBBS, logger, correctStack, schedulerAddr, fakeClient)
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
				var allocateCalled bool
				var deletedContainerGuid string

				BeforeEach(func() {
					allocateCalled = false
					deletedContainerGuid = ""

					fakeClient.WhenAllocatingContainer = func(req client.ContainerRequest) (client.ContainerResponse, error) {
						defer GinkgoRecover()

						allocateCalled = true
						Ω(fakeBBS.ClaimedTasks()).Should(HaveLen(0))
						Ω(req.MemoryMB).Should(Equal(64))
						Ω(req.DiskMB).Should(Equal(1024))
						Ω(req.CpuPercent).Should(Equal(0.5))
						Ω(req.LogConfig).Should(Equal(task.Log))
						return client.ContainerResponse{ExecutorGuid: "the-executor-guid", Guid: "guid-123", ContainerRequest: req}, nil
					}

					fakeClient.WhenDeletingContainer = func(allocationGuid string) error {
						deletedContainerGuid = allocationGuid
						return nil
					}
				})

				Context("when claiming the task succeeds", func() {
					Context("when initializing the container succeeds", func() {
						var initCalled bool

						BeforeEach(func() {
							initCalled = false

							fakeClient.WhenInitializingContainer = func(allocationGuid string) error {
								defer GinkgoRecover()

								initCalled = true
								Ω(allocationGuid).Should(Equal("guid-123"))
								Ω(fakeBBS.ClaimedTasks()).Should(HaveLen(1))
								Ω(fakeBBS.StartedTasks()).Should(HaveLen(0))
								return nil
							}
						})

						Context("and the executor successfully starts running the task", func() {
							var (
								completionURL string
								sentMetadata  []byte
								runCalled     bool
							)

							BeforeEach(func() {
								completionURL = ""
								runCalled = false
								sentMetadata = []byte{}

								fakeClient.WhenRunning = func(allocationGuid string, req client.RunRequest) error {
									defer GinkgoRecover()

									runCalled = true

									Ω(fakeBBS.StartedTasks()).Should(HaveLen(1))

									expectedTask := task
									expectedTask.ExecutorID = "the-executor-guid"
									expectedTask.ContainerHandle = "guid-123"
									Ω(fakeBBS.StartedTasks()[0]).Should(Equal(expectedTask))

									Ω(allocationGuid).Should(Equal("guid-123"))
									Ω(req.Actions).Should(Equal(task.Actions))

									sentMetadata = req.Metadata

									completionURL = req.CompletionURL
									return nil
								}
							})

							It("makes all calls to the executor", func() {
								Eventually(ValueOf(&allocateCalled)).Should(BeTrue())
								Eventually(ValueOf(&initCalled)).Should(BeTrue())
								Eventually(ValueOf(&runCalled)).Should(BeTrue())
							})

							Describe("and the task succeeds", func() {
								var resp *http.Response
								var err error

								sendCompletionCallback := func() {
									body, jsonErr := fake_client.MarshalContainerRunResult(client.ContainerRunResult{
										Result:   "42",
										Metadata: sentMetadata,
									})

									Ω(jsonErr).ShouldNot(HaveOccurred())

									Eventually(ValueOf(&completionURL)).ShouldNot(BeEmpty())
									resp, err = http.Post(completionURL, "application/json", bytes.NewReader(body))
								}

								It("responds to the onComplete hook", func() {
									Eventually(ValueOf(&runCalled)).Should(BeTrue())

									sendCompletionCallback()

									Ω(err).ShouldNot(HaveOccurred())
									Ω(resp.StatusCode).Should(Equal(http.StatusOK))
								})

								It("records the job result", func() {
									Eventually(ValueOf(&runCalled)).Should(BeTrue())

									sendCompletionCallback()

									task.Result = "42"
									Eventually(fakeBBS.CompletedTasks).Should(HaveLen(1))
									Ω(fakeBBS.CompletedTasks()[0].Guid).Should(Equal(task.Guid))
								})
							})

							Describe("and the task fails", func() {
								var resp *http.Response
								var err error

								sendCompletionCallback := func() {
									body, jsonErr := fake_client.MarshalContainerRunResult(client.ContainerRunResult{
										Failed:        true,
										FailureReason: "it didn't work",
										Metadata:      sentMetadata,
									})
									Ω(jsonErr).ShouldNot(HaveOccurred())

									resp, err = http.Post(fmt.Sprintf("http://%s/complete", schedulerAddr), "application/json", bytes.NewReader(body))
								}

								It("responds to the onComplete hook", func() {
									Eventually(ValueOf(&runCalled)).Should(BeTrue())

									sendCompletionCallback()

									Ω(err).ShouldNot(HaveOccurred())
									Ω(resp.StatusCode).Should(Equal(http.StatusOK))
								})

								It("records the job failure", func() {
									Eventually(ValueOf(&runCalled)).Should(BeTrue())

									sendCompletionCallback()

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
								Eventually(ValueOf(&deletedContainerGuid)).Should(Equal("guid-123"))
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
							Eventually(ValueOf(&deletedContainerGuid)).Should(Equal("guid-123"))
						})
					})
				})

				Context("but claiming the task fails", func() {
					BeforeEach(func() {
						fakeBBS.SetClaimTaskErr(errors.New("data store went away."))
					})

					It("deletes the resource allocation on the executor", func() {
						Eventually(ValueOf(&deletedContainerGuid)).Should(Equal("guid-123"))
					})
				})
			})

			Context("when reserving the container fails", func() {
				var allocatedContainer bool

				BeforeEach(func() {
					allocatedContainer = false

					fakeClient.WhenAllocatingContainer = func(req client.ContainerRequest) (client.ContainerResponse, error) {
						allocatedContainer = true
						return client.ContainerResponse{}, errors.New("Something went wrong")
					}
				})

				It("makes the resource allocation request", func() {
					Eventually(ValueOf(&allocatedContainer)).Should(BeTrue())
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

func ValueOf(p interface{}) func() interface{} {
	return func() interface{} { return reflect.Indirect(reflect.ValueOf(p)).Interface() }
}
