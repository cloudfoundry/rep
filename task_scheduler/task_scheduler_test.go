package task_scheduler_test

import (
	"errors"
	"syscall"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/rata"

	fake_client "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep/routes"
	"github.com/cloudfoundry-incubator/rep/task_scheduler"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/onsi/gomega/ghttp"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const claimWaitTimeout = task_scheduler.MaxClaimWaitInMillis * time.Millisecond * 2

var _ = Describe("TaskScheduler", func() {
	var logger lager.Logger

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test")
	})

	Context("when a task scheduler is running", func() {
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
			fakeClient = new(fake_client.FakeClient)
			fakeExecutor = ghttp.NewServer()
			fakeBBS = &fake_bbs.FakeRepBBS{}

			desiredTaskChan = make(chan models.Task, 0)
			watchStopChan = make(chan bool, 0)
			watchErrorChan = make(chan error, 0)

			fakeBBS.WatchForDesiredTaskReturns(desiredTaskChan, watchStopChan, watchErrorChan)

			task = models.Task{
				TaskGuid:   "task-guid-123",
				Stack:      correctStack,
				RootFSPath: "the-rootfs-path",
				MemoryMB:   64,
				DiskMB:     1024,
				CPUWeight:  5,
				Actions: []models.ExecutorAction{
					{
						Action: models.RunAction{
							Path:    "the-script",
							Env:     []models.EnvironmentVariable{{Name: "PATH", Value: "the-path"}},
							Timeout: 500,
						},
					},
				},
				Log: models.LogConfig{
					Guid:       "some-guid",
					SourceName: "XYZ",
				},
			}

		})

		BeforeEach(func() {
			taskScheduler = ifrit.Envoke(task_scheduler.New(
				"some-executor-id",
				rata.NewRequestGenerator(
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

				fakeClient.AllocateContainerReturns(executor.Container{}, errors.New("Failed to allocate"))
			})

			JustBeforeEach(func() {
				errorTime = time.Now()
				watchErrorChan <- errors.New("Failed to watch for task")
				desiredTaskChan <- task
			})

			It("should wait 3 seconds and retry", func() {
				Eventually(fakeClient.AllocateContainerCallCount, 5).Should(Equal(1))
				Ω(time.Now().Sub(errorTime)).Should(BeNumerically("~", 3*time.Second, 200*time.Millisecond))
			})
		})

		Context("when a task is desired", func() {
			JustBeforeEach(func() {
				desiredTaskChan <- task
			})

			Context("when reserving the container succeeds", func() {
				var allocateCalled chan struct{}
				var deletedContainerGuid chan string

				BeforeEach(func() {
					allocateCalled = make(chan struct{}, 1)
					deletedContainerGuid = make(chan string, 1)

					fakeClient.AllocateContainerStub = func(containerGuid string, req executor.Container) (executor.Container, error) {
						defer GinkgoRecover()

						allocateCalled <- struct{}{}
						Ω(fakeBBS.ClaimTaskCallCount()).Should(Equal(0))

						Ω(containerGuid).Should(Equal(task.TaskGuid))
						Ω(req.Guid).Should(Equal(containerGuid))
						Ω(req.MemoryMB).Should(Equal(64))
						Ω(req.DiskMB).Should(Equal(1024))
						Ω(req.RootFSPath).Should(Equal("the-rootfs-path"))
						Ω(req.CPUWeight).Should(Equal(uint(5)))
						Ω(req.Log).Should(Equal(executor.LogConfig{
							Guid:       task.Log.Guid,
							SourceName: task.Log.SourceName,
						}))
						Ω(req.Actions).Should(Equal(task.Actions))

						return executor.Container{Guid: containerGuid}, nil
					}

					fakeClient.DeleteContainerStub = func(allocationGuid string) error {
						deletedContainerGuid <- allocationGuid
						return nil
					}
				})

				It("should claim the task", func() {
					Eventually(fakeBBS.ClaimTaskCallCount, claimWaitTimeout).Should(Equal(1))
					taskGuid, executorGuid := fakeBBS.ClaimTaskArgsForCall(0)
					Ω(taskGuid).Should(Equal(task.TaskGuid))
					Ω(executorGuid).Should(Equal("some-executor-id"))
				})

				Context("when claiming the task succeeds", func() {
					Context("when running the container succeeds", func() {
						BeforeEach(func() {
							fakeClient.RunContainerStub = func(allocationGuid string) error {
								defer GinkgoRecover()

								Ω(allocationGuid).Should(Equal(task.TaskGuid))

								Ω(fakeBBS.ClaimTaskCallCount()).Should(Equal(1))
								Ω(fakeBBS.StartTaskCallCount()).Should(Equal(0))

								return nil
							}
						})

						It("should start the task", func() {
							Eventually(fakeBBS.StartTaskCallCount, claimWaitTimeout).Should(Equal(1))

							taskGuid, executorGuid := fakeBBS.StartTaskArgsForCall(0)
							Ω(taskGuid).Should(Equal(task.TaskGuid))
							Ω(executorGuid).Should(Equal("some-executor-id"))
						})

						Context("but starting the task fails", func() {
							BeforeEach(func() {
								fakeBBS.StartTaskReturns(errors.New("kerpow"))
							})

							It("deletes the container", func() {
								Eventually(deletedContainerGuid, claimWaitTimeout).Should(Receive(Equal(task.TaskGuid)))
							})
						})
					})

					Context("but running the container fails", func() {
						BeforeEach(func() {
							fakeClient.RunContainerReturns(errors.New("oh no!"))
						})

						It("does not mark the job as started", func() {
							Consistently(fakeBBS.StartTaskCallCount).Should(Equal(0))
						})

						It("deletes the container", func() {
							Eventually(deletedContainerGuid, claimWaitTimeout).Should(Receive(Equal(task.TaskGuid)))
						})

						It("marks the task as failed", func() {
							Eventually(fakeBBS.CompleteTaskCallCount, claimWaitTimeout).Should(Equal(1))
							taskGuid, failed, failureReason, _ := fakeBBS.CompleteTaskArgsForCall(0)
							Ω(taskGuid).Should(Equal(task.TaskGuid))
							Ω(failed).Should(BeTrue())
							Ω(failureReason).Should(ContainSubstring("failed to run container - oh no!"))
						})
					})
				})

				Context("but claiming the task fails", func() {
					BeforeEach(func() {
						fakeBBS.ClaimTaskReturns(errors.New("data store went away."))
					})

					It("deletes the resource allocation on the executor", func() {
						Eventually(deletedContainerGuid, claimWaitTimeout).Should(Receive(Equal(task.TaskGuid)))
					})
				})
			})

			Context("when reserving the container fails", func() {
				BeforeEach(func() {
					fakeClient.AllocateContainerReturns(executor.Container{}, errors.New("Something went wrong"))
				})

				It("makes the resource allocation request", func() {
					Eventually(fakeClient.AllocateContainerCallCount).Should(Equal(1))
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
					TaskGuid:  "task-guid-123",
					Stack:     "asd;oubhasdfbuvasfb",
					MemoryMB:  64,
					DiskMB:    1024,
					CPUWeight: 5,
					Actions:   []models.ExecutorAction{},
				}

				desiredTaskChan <- task
			})

			It("ignores the task", func() {
				Consistently(fakeBBS.ClaimTaskCallCount).Should(Equal(0))
			})
		})
	})
})
