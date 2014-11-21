package reaper_test

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep/gatherer/fake_gatherer"
	"github.com/cloudfoundry-incubator/rep/reaper"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("TaskCompleter", func() {
	var (
		taskCompleter *reaper.TaskCompleter
		bbs           *fake_bbs.FakeRepBBS
		snapshot      *fake_gatherer.FakeSnapshot
	)

	BeforeEach(func() {
		bbs = new(fake_bbs.FakeRepBBS)
		snapshot = new(fake_gatherer.FakeSnapshot)
	})

	JustBeforeEach(func() {
		taskCompleter = reaper.NewTaskCompleter(bbs, lagertest.NewTestLogger("test"))
		taskCompleter.Process(snapshot)
	})

	Describe("updating the BBS when there are missing containers", func() {
		Context("when there are claimed/running tasks for this executor in the BBS", func() {
			BeforeEach(func() {
				snapshot.TasksReturns([]models.Task{
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
				})
			})

			Context("but the executor doesn't know about these tasks", func() {
				BeforeEach(func() {
					snapshot.GetContainerReturns(nil, false)
				})

				It("marks those tasks as complete & failed", func() {
					Eventually(bbs.CompleteTaskCallCount()).Should(Equal(2))

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
					snapshot.GetContainerReturns(&executor.Container{}, true)
				})

				It("does not mark those tasks as complete", func() {
					Consistently(bbs.CompleteTaskCallCount()).Should(Equal(0))
				})
			})
		})

		Context("when there are completed tasks associated with this executor in the BBS", func() {
			BeforeEach(func() {
				snapshot.TasksReturns([]models.Task{
					models.Task{
						TaskGuid: "task-guid-1",
						State:    models.TaskStateCompleted,
					},
					models.Task{
						TaskGuid: "task-guid-2",
						State:    models.TaskStateCompleted,
					},
				})
			})

			It("does not try to mark those tasks as completed & failed", func() {
				Consistently(bbs.CompleteTaskCallCount()).Should(Equal(0))
			})
		})
	})
})
