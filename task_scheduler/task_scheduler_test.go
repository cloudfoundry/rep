package task_scheduler_test

import (
	"errors"
	"syscall"
	"time"

	"github.com/cloudfoundry-incubator/executor/api"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/router"

	"github.com/cloudfoundry-incubator/executor/client/fake_client"
	"github.com/cloudfoundry-incubator/rep/routes"
	"github.com/cloudfoundry-incubator/rep/task_scheduler"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gosteno"
	"github.com/onsi/gomega/ghttp"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("TaskScheduler", func() {
	var logger *gosteno.Logger

	BeforeSuite(func() {
		gosteno.EnterTestMode(gosteno.LOG_DEBUG)
	})

	BeforeEach(func() {
		logger = gosteno.NewLogger("test-logger")
	})

	Context("when a game scheduler is running", func() {
		var (
			fakeExecutor  *ghttp.Server
			fakeBBS       *fake_bbs.FakeRepBBS
			taskScheduler ifrit.Process
			correctStack  = "my-stack"
			fakeClient    *fake_client.FakeClient
			task          models.Task

			desiredTaskChan chan models.Task
			watchStopChan   chan bool
			watchErrorChan  chan error
		)

		BeforeEach(func() {
			fakeClient = fake_client.New()
			fakeExecutor = ghttp.NewServer()
			fakeBBS = &fake_bbs.FakeRepBBS{}

			desiredTaskChan = make(chan models.Task, 0)
			watchStopChan = make(chan bool, 0)
			watchErrorChan = make(chan error, 0)

			fakeBBS.WatchForDesiredTaskReturns(desiredTaskChan, watchStopChan, watchErrorChan)

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

		})

		BeforeEach(func() {
			taskScheduler = ifrit.Envoke(task_scheduler.New(
				router.NewRequestGenerator(
					routes.TaskCompleted,
					routes.Routes,
				),
				fakeBBS,
				logger,
				correctStack,
				fakeClient,
			))
		})

		AfterEach(func() {
			taskScheduler.Signal(syscall.SIGTERM)
			<-taskScheduler.Wait()
			fakeExecutor.Close()
		})

		Context("when watching for desired task fails", func() {
			var errorTime time.Time
			var allocationTimeChan chan time.Time

			BeforeEach(func() {
				allocationTimeChan = make(chan time.Time)

				fakeClient.WhenAllocatingContainer = func(containerGuid string, req api.ContainerAllocationRequest) (api.Container, error) {
					allocationTimeChan <- time.Now()
					return api.Container{}, errors.New("Failed to allocate")
				}
			})

			JustBeforeEach(func() {
				errorTime = time.Now()
				watchErrorChan <- errors.New("Failed to watch for task")
				desiredTaskChan <- task
			})

			It("should wait 3 seconds and retry", func() {
				var allocationTime time.Time
				Eventually(allocationTimeChan, 5).Should(Receive(&allocationTime))
				Ω(allocationTime.Sub(errorTime)).Should(BeNumerically("~", 3*time.Second, 200*time.Millisecond))
			})

		})

		Context("when a staging task is desired", func() {
			JustBeforeEach(func() {
				desiredTaskChan <- task
			})

			Context("when reserving the container succeeds", func() {
				var allocateCalled chan struct{}
				var deletedContainerGuid chan string

				BeforeEach(func() {
					allocateCalled = make(chan struct{}, 1)
					deletedContainerGuid = make(chan string, 1)

					fakeClient.WhenAllocatingContainer = func(containerGuid string, req api.ContainerAllocationRequest) (api.Container, error) {
						defer GinkgoRecover()

						allocateCalled <- struct{}{}
						Ω(fakeBBS.ClaimTaskCallCount()).Should(Equal(0))
						Ω(req.MemoryMB).Should(Equal(64))
						Ω(req.DiskMB).Should(Equal(1024))
						Ω(containerGuid).Should(Equal(task.Guid))
						return api.Container{ExecutorGuid: "the-executor-guid", Guid: containerGuid}, nil
					}

					fakeClient.WhenDeletingContainer = func(allocationGuid string) error {
						deletedContainerGuid <- allocationGuid
						return nil
					}
				})

				It("should claim the task", func() {
					Eventually(fakeBBS.ClaimTaskCallCount).Should(Equal(1))
					taskGuid, executorGuid := fakeBBS.ClaimTaskArgsForCall(0)
					Ω(taskGuid).Should(Equal(task.Guid))
					Ω(executorGuid).Should(Equal("the-executor-guid"))
				})

				Context("when claiming the task succeeds", func() {
					Context("when initializing the container succeeds", func() {
						var initCalled chan struct{}

						BeforeEach(func() {
							initCalled = make(chan struct{}, 1)

							fakeClient.WhenInitializingContainer = func(allocationGuid string, req api.ContainerInitializationRequest) (api.Container, error) {
								defer GinkgoRecover()

								initCalled <- struct{}{}
								Ω(allocationGuid).Should(Equal(task.Guid))
								Ω(req.CpuPercent).Should(Equal(0.5))
								Ω(req.Log).Should(Equal(task.Log))

								Ω(fakeBBS.ClaimTaskCallCount()).Should(Equal(1))
								Ω(fakeBBS.StartTaskCallCount()).Should(Equal(0))
								return api.Container{ExecutorGuid: "the-executor-guid", ContainerHandle: "the-container-handle"}, nil
							}
						})

						It("should start the task", func() {
							Eventually(fakeBBS.StartTaskCallCount).Should(Equal(1))
							taskGuid, executorGuid, containerHandle := fakeBBS.StartTaskArgsForCall(0)
							Ω(taskGuid).Should(Equal(task.Guid))
							Ω(executorGuid).Should(Equal("the-executor-guid"))
							Ω(containerHandle).Should(Equal("the-container-handle"))
						})

						Context("and the executor successfully starts running the task", func() {
							var (
								reqChan chan api.ContainerRunRequest
							)

							BeforeEach(func() {
								reqChan = make(chan api.ContainerRunRequest, 1)

								fakeClient.WhenRunning = func(allocationGuid string, req api.ContainerRunRequest) error {
									defer GinkgoRecover()

									Ω(fakeBBS.StartTaskCallCount()).Should(Equal(1))

									Ω(allocationGuid).Should(Equal(task.Guid))
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
						})

						Context("but starting the task fails", func() {
							BeforeEach(func() {
								fakeBBS.StartTaskReturns(errors.New("kerpow"))
							})

							It("deletes the container", func() {
								Eventually(deletedContainerGuid).Should(Receive(Equal(task.Guid)))
							})
						})
					})

					Context("but initializing the container fails", func() {
						BeforeEach(func() {
							fakeClient.WhenInitializingContainer = func(allocationGuid string, req api.ContainerInitializationRequest) (api.Container, error) {
								return api.Container{}, errors.New("Can't initialize")
							}
						})

						It("does not mark the job as started", func() {
							Consistently(fakeBBS.StartTaskCallCount).Should(Equal(0))
						})

						It("deletes the container", func() {
							Eventually(deletedContainerGuid).Should(Receive(Equal(task.Guid)))
						})

						It("marks the task as failed", func() {
							Eventually(fakeBBS.CompleteTaskCallCount).Should(Equal(1))
							taskGuid, failed, failureReason, _ := fakeBBS.CompleteTaskArgsForCall(0)
							Ω(taskGuid).Should(Equal(task.Guid))
							Ω(failed).Should(BeTrue())
							Ω(failureReason).Should(ContainSubstring("Failed to initialize container - Can't initialize"))
						})
					})
				})

				Context("but claiming the task fails", func() {
					BeforeEach(func() {
						fakeBBS.ClaimTaskReturns(errors.New("data store went away."))
					})

					It("deletes the resource allocation on the executor", func() {
						Eventually(deletedContainerGuid).Should(Receive(Equal(task.Guid)))
					})
				})
			})

			Context("when reserving the container fails", func() {
				var allocatedContainer chan struct{}

				BeforeEach(func() {
					allocatedContainer = make(chan struct{}, 1)

					fakeClient.WhenAllocatingContainer = func(guid string, req api.ContainerAllocationRequest) (api.Container, error) {
						allocatedContainer <- struct{}{}
						return api.Container{}, errors.New("Something went wrong")
					}
				})

				It("makes the resource allocation request", func() {
					Eventually(allocatedContainer).Should(Receive())
				})

				It("does not mark the job as Claimed", func() {
					Consistently(fakeBBS.ClaimTaskCallCount).Should(Equal(0))
				})

				It("does not mark the job as Started", func() {
					Consistently(fakeBBS.StartTaskCallCount).Should(Equal(0))
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

				desiredTaskChan <- task
			})

			It("ignores the task", func() {
				Consistently(fakeBBS.ClaimTaskCallCount).Should(Equal(0))
			})
		})
	})
})
