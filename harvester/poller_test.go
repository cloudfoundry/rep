package harvester_test

import (
	"errors"
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	efakes "github.com/cloudfoundry-incubator/executor/fakes"
	. "github.com/cloudfoundry-incubator/rep/harvester"
	"github.com/cloudfoundry-incubator/rep/harvester/fakes"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/pivotal-golang/timer/fake_timer"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Poller", func() {
	var (
		executorClient *efakes.FakeClient
		processor      *fakes.FakeProcessor

		pollInterval time.Duration
		timer        *fake_timer.FakeTimer
		poller       ifrit.Runner
		process      ifrit.Process
		bbs          *fake_bbs.FakeRepBBS
	)

	BeforeEach(func() {
		pollInterval = 100 * time.Millisecond
		timer = fake_timer.NewFakeTimer(time.Now())
		executorClient = new(efakes.FakeClient)
		processor = new(fakes.FakeProcessor)

		bbs = new(fake_bbs.FakeRepBBS)
		poller = NewPoller(pollInterval, timer, executorClient, processor, bbs, "executor-id", lagertest.NewTestLogger("test"))
	})

	JustBeforeEach(func() {
		process = ifrit.Invoke(poller)
	})

	AfterEach(func() {
		process.Signal(os.Interrupt)
		Eventually(process.Wait()).Should(Receive())
	})

	Context("when the timer elapses", func() {
		JustBeforeEach(func() {
			timer.Elapse(pollInterval)
		})

		It("polls executor for tasks", func() {
			Eventually(executorClient.ListContainersCallCount).Should(Equal(1))

			Ω(executorClient.ListContainersArgsForCall(0)).Should(Equal(executor.Tags{
				LifecycleTag: TaskLifecycle,
			}))
		})

		Context("and the executor returns completed containers", func() {
			BeforeEach(func() {
				executorClient.ListContainersReturns([]executor.Container{
					{Guid: "first-completed-guid", State: executor.StateCompleted},
					{Guid: "second-completed-guid", State: executor.StateCompleted},
					{Guid: "created-guid", State: executor.StateCreated},
					{Guid: "initializing-guid", State: executor.StateInitializing},
					{Guid: "reserved-guid", State: executor.StateReserved},
				}, nil)
			})

			It("processes each of them", func() {
				Eventually(processor.ProcessCallCount).Should(Equal(2))

				Ω(processor.ProcessArgsForCall(0)).Should(Equal(executor.Container{
					Guid:  "first-completed-guid",
					State: executor.StateCompleted,
				}))

				Ω(processor.ProcessArgsForCall(1)).Should(Equal(executor.Container{
					Guid:  "second-completed-guid",
					State: executor.StateCompleted,
				}))
			})

			It("does not process the non-completed containers", func() {
				Eventually(processor.ProcessCallCount).Should(Equal(2))
				Consistently(processor.ProcessCallCount).Should(Equal(2))
			})
		})

		Context("when the executor returns no completed containers", func() {
			BeforeEach(func() {
				executorClient.ListContainersReturns([]executor.Container{
					{Guid: "created-guid", State: executor.StateCreated},
					{Guid: "initializing-guid", State: executor.StateInitializing},
					{Guid: "reserved-guid", State: executor.StateReserved},
				}, nil)
			})

			It("doesn't process anything", func() {
				Consistently(processor.ProcessCallCount()).Should(BeZero())
			})
		})

		Context("when listing containers fails", func() {
			disaster := errors.New("oh no!")

			BeforeEach(func() {
				executorClient.ListContainersReturns(nil, disaster)
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
					executorClient.ListContainersReturns([]executor.Container{}, nil)
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
					executorClient.ListContainersReturns([]executor.Container{
						{Guid: "task-guid-1"},
						{Guid: "task-guid-2"},
					}, nil)
				})
				It("does not mark those tasks as complete", func() {
					Consistently(bbs.CompleteTaskCallCount).Should(Equal(0))
				})
			})

			Context("when listing containers fails", func() {
				BeforeEach(func() {
					executorClient.ListContainersReturns(nil, errors.New("executor error"))
				})
				It("does not mark those tasks as complete", func() {
					Consistently(bbs.CompleteTaskCallCount).Should(Equal(0))
				})
				It("does not query the BBS for tasks", func() {
					Consistently(bbs.GetAllTasksByExecutorIDCallCount).Should(Equal(0))
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
