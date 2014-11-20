package reaper_test

import (
	"errors"
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	efakes "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/reaper"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/storeadapter"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/pivotal-golang/timer/fake_timer"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Task Reaper", func() {
	var (
		executorClient *efakes.FakeClient

		pollInterval time.Duration
		timer        *fake_timer.FakeTimer
		taskReaper   ifrit.Runner
		process      ifrit.Process
		bbs          *fake_bbs.FakeRepBBS
	)

	BeforeEach(func() {
		pollInterval = 100 * time.Millisecond
		timer = fake_timer.NewFakeTimer(time.Now())
		executorClient = new(efakes.FakeClient)

		bbs = new(fake_bbs.FakeRepBBS)
		taskReaper = reaper.NewTaskReaper(pollInterval, timer, "cell-id", bbs, executorClient, lagertest.NewTestLogger("test"))
	})

	JustBeforeEach(func() {
		process = ifrit.Invoke(taskReaper)
	})

	AfterEach(func() {
		process.Signal(os.Interrupt)
		Eventually(process.Wait()).Should(Receive())
	})

	Context("when the timer elapses", func() {
		JustBeforeEach(func() {
			timer.Elapse(pollInterval)
		})

		Describe("updating the BBS when there are missing containers", func() {
			Context("when there are claimed/running tasks for this executor in the BBS", func() {
				BeforeEach(func() {
					bbs.TasksByCellIDReturns([]models.Task{
						models.Task{
							TaskGuid: "task-guid-1",
							State:    models.TaskStateClaimed,
							Action: &models.RunAction{
								Path: "ls",
							},
						},
						models.Task{
							TaskGuid: "task-guid-2",
							State:    models.TaskStateRunning,
							Action: &models.RunAction{
								Path: "ls",
							},
						},
					}, nil)
				})

				Context("but the executor doesn't know about these tasks", func() {
					BeforeEach(func() {
						executorClient.GetContainerReturns(executor.Container{}, executor.ErrContainerNotFound)
					})

					It("marks those tasks as complete & failed", func() {
						Eventually(bbs.CompleteTaskCallCount).Should(Equal(2))

						taskGuid, failed, failureReason, _ := bbs.CompleteTaskArgsForCall(0)
						Ω(taskGuid).Should(Equal("task-guid-1"))
						Ω(failed).Should(BeTrue())
						Ω(failureReason).Should(Equal("task container no longer exists"))

						taskGuid, failed, failureReason, _ = bbs.CompleteTaskArgsForCall(1)
						Ω(taskGuid).Should(Equal("task-guid-2"))
						Ω(failed).Should(BeTrue())
						Ω(failureReason).Should(Equal("task container no longer exists"))
					})
				})

				Context("and the executor does know about these tasks", func() {
					BeforeEach(func() {
						executorClient.GetContainerReturns(executor.Container{}, nil)
					})

					It("does not mark those tasks as complete", func() {
						Consistently(bbs.CompleteTaskCallCount).Should(Equal(0))
					})
				})

				Context("when getting the container fails for some reason", func() {
					BeforeEach(func() {
						executorClient.GetContainerReturns(executor.Container{}, errors.New("executor error"))
					})

					It("does not mark those tasks as complete", func() {
						Consistently(bbs.CompleteTaskCallCount).Should(Equal(0))
					})
				})
			})

			Context("when getting tasks from the BBS fails", func() {
				BeforeEach(func() {
					bbs.TasksByCellIDReturns(nil, errors.New("bbs error"))
				})
				It("does not die", func() {
					Consistently(process.Wait()).ShouldNot(Receive())
				})

				Context("and the timer elapses again", func() {
					JustBeforeEach(func() {
						timer.Elapse(pollInterval)
					})

					It("happily continues on to next time", func() {
						Eventually(bbs.TasksByCellIDCallCount).Should(Equal(2))
					})
				})
			})

			Context("when there are completed tasks associated with this executor in the BBS", func() {
				BeforeEach(func() {
					bbs.TasksByCellIDReturns([]models.Task{
						models.Task{
							TaskGuid: "task-guid-1",
							State:    models.TaskStateCompleted,
						},
						models.Task{
							TaskGuid: "task-guid-2",
							State:    models.TaskStateCompleted,
						},
					}, nil)
				})

				It("does not try to mark those tasks as completed & failed", func() {
					Consistently(bbs.CompleteTaskCallCount).Should(Equal(0))
				})
			})
		})

		Describe("deleting containers which are not supposed to be running anymore", func() {
			It("queries task containers", func() {
				Eventually(executorClient.ListContainersCallCount).Should(Equal(1))

				Ω(executorClient.ListContainersArgsForCall(0)).To(Equal(executor.Tags{
					rep.LifecycleTag: rep.TaskLifecycle,
				}))
			})

			Context("when there is a container", func() {
				BeforeEach(func() {
					executorClient.ListContainersReturns([]executor.Container{
						executor.Container{Guid: "some-task-guid"},
					}, nil)
				})

				Context("which belongs to a pending task", func() {
					BeforeEach(func() {
						bbs.TaskByGuidStub = func(taskGuid string) (*models.Task, error) {
							defer GinkgoRecover()
							Ω(taskGuid).Should(Equal("some-task-guid"))
							return &models.Task{State: models.TaskStatePending}, nil
						}
					})

					It("does not delete the container", func() {
						Consistently(executorClient.DeleteContainerCallCount).Should(Equal(0))
					})
				})

				Context("which belongs to a completed task", func() {
					BeforeEach(func() {
						bbs.TaskByGuidStub = func(taskGuid string) (*models.Task, error) {
							defer GinkgoRecover()
							Ω(taskGuid).Should(Equal("some-task-guid"))
							return &models.Task{
								State: models.TaskStateCompleted,
								Action: &models.RunAction{
									Path: "ls",
								},
							}, nil
						}
					})

					It("deletes the container", func() {
						Eventually(executorClient.DeleteContainerCallCount).Should(Equal(1))
						Ω(executorClient.DeleteContainerArgsForCall(0)).Should(Equal("some-task-guid"))
					})

					Context("when deleting the container fails", func() {
						BeforeEach(func() {
							executorClient.DeleteContainerReturns(errors.New("delete error"))
						})

						It("does not die", func() {
							Consistently(process.Wait()).ShouldNot(Receive())
						})

						Context("and the timer elapses again", func() {
							JustBeforeEach(func() {
								timer.Elapse(pollInterval)
							})

							It("happily continues on to next time", func() {
								Eventually(executorClient.ListContainersCallCount).Should(Equal(2))
							})
						})
					})
				})

				Context("which belongs to a resolving task", func() {
					BeforeEach(func() {
						bbs.TaskByGuidStub = func(taskGuid string) (*models.Task, error) {
							defer GinkgoRecover()
							Ω(taskGuid).Should(Equal("some-task-guid"))
							return &models.Task{
								State: models.TaskStateResolving,
								Action: &models.RunAction{
									Path: "ls",
								},
							}, nil
						}
					})

					It("deletes the container", func() {
						Eventually(executorClient.DeleteContainerCallCount).Should(Equal(1))
						Ω(executorClient.DeleteContainerArgsForCall(0)).Should(Equal("some-task-guid"))
					})
				})

				Context("for which there is no associated task", func() {
					BeforeEach(func() {
						bbs.TaskByGuidStub = func(taskGuid string) (*models.Task, error) {
							defer GinkgoRecover()
							Ω(taskGuid).Should(Equal("some-task-guid"))
							return nil, storeadapter.ErrorKeyNotFound
						}
					})

					It("deletes the container", func() {
						Eventually(executorClient.DeleteContainerCallCount).Should(Equal(1))
						Ω(executorClient.DeleteContainerArgsForCall(0)).Should(Equal("some-task-guid"))
					})
				})

				Context("for which the BBS lookup fails for some other reason", func() {
					BeforeEach(func() {
						bbs.TaskByGuidReturns(nil, errors.New("bbs error"))
					})

					It("does not delete the container", func() {
						Consistently(executorClient.DeleteContainerCallCount).Should(Equal(0))
					})
				})
			})

			Context("when getting containers fails", func() {
				BeforeEach(func() {
					executorClient.ListContainersReturns(nil, errors.New("executor error"))
				})

				It("does not die", func() {
					Consistently(process.Wait()).ShouldNot(Receive())
				})

				Context("and the timer elapses again", func() {
					JustBeforeEach(func() {
						timer.Elapse(pollInterval)
					})

					It("happily continues on to next time", func() {
						Eventually(executorClient.ListContainersCallCount).Should(Equal(2))
					})
				})
			})
		})
	})
})
