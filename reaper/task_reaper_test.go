package reaper_test

import (
	"errors"
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	efakes "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep/reaper"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
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
		taskReaper = reaper.NewTaskReaper(pollInterval, timer, "executor-id", bbs, executorClient, lagertest.NewTestLogger("test"))
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

		It("gets tasks for this executor from the BBS", func() {
			Eventually(bbs.GetAllTasksByExecutorIDCallCount).Should(Equal(1))
			Ω(bbs.GetAllTasksByExecutorIDArgsForCall(0)).Should(Equal("executor-id"))
		})

		Context("when there are claimed/running tasks for this executor in the BBS", func() {
			BeforeEach(func() {
				bbs.GetAllTasksByExecutorIDReturns([]models.Task{
					models.Task{
						TaskGuid: "task-guid-1",
						State:    models.TaskStateClaimed,
					},
					models.Task{
						TaskGuid: "task-guid-2",
						State:    models.TaskStateRunning,
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

			Context("when get container fails for some reason", func() {
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
				bbs.GetAllTasksByExecutorIDReturns(nil, errors.New("bbs error"))
			})
			It("does not die", func() {
				Consistently(process.Wait()).ShouldNot(Receive())
			})

			Context("and the timer elapses again", func() {
				JustBeforeEach(func() {
					timer.Elapse(pollInterval)
				})

				It("happily continues on to next time", func() {
					Eventually(bbs.GetAllTasksByExecutorIDCallCount).Should(Equal(2))
				})
			})
		})

		Context("when there are completed tasks associated with this executor in the BBS", func() {
			BeforeEach(func() {
				bbs.GetAllTasksByExecutorIDReturns([]models.Task{
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
})
