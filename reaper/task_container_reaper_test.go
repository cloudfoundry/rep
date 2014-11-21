package reaper_test

import (
	"github.com/cloudfoundry-incubator/executor"
	efakes "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/gatherer/fake_gatherer"
	"github.com/cloudfoundry-incubator/rep/reaper"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("TaskContainerReaper", func() {
	var (
		taskContainerReaper *reaper.TaskContainerReaper
		executorClient      *efakes.FakeClient
		snapshot            *fake_gatherer.FakeSnapshot
	)

	BeforeEach(func() {
		executorClient = new(efakes.FakeClient)
		snapshot = new(fake_gatherer.FakeSnapshot)
	})

	JustBeforeEach(func() {
		taskContainerReaper = reaper.NewTaskContainerReaper(executorClient, lagertest.NewTestLogger("test"))
		taskContainerReaper.Process(snapshot)
	})

	Describe("deleting containers which are not supposed to be running anymore", func() {
		It("queries task containers", func() {
			Ω(snapshot.ListContainersCallCount()).Should(Equal(1))

			Ω(snapshot.ListContainersArgsForCall(0)).To(Equal(executor.Tags{
				rep.LifecycleTag: rep.TaskLifecycle,
			}))
		})

		Context("when there is a container", func() {
			BeforeEach(func() {
				snapshot.ListContainersReturns([]executor.Container{
					executor.Container{Guid: "some-task-guid"},
				})
			})

			Context("which belongs to a pending task", func() {
				BeforeEach(func() {
					snapshot.GetTaskStub = func(taskGuid string) *models.Task {
						Ω(taskGuid).Should(Equal("some-task-guid"))
						return &models.Task{State: models.TaskStatePending}
					}
				})

				It("does not delete the container", func() {
					Ω(executorClient.DeleteContainerCallCount()).Should(Equal(0))
				})
			})

			Context("which belongs to a completed task", func() {
				BeforeEach(func() {
					snapshot.GetTaskStub = func(taskGuid string) *models.Task {
						Ω(taskGuid).Should(Equal("some-task-guid"))
						return &models.Task{
							State: models.TaskStateCompleted,
							Action: &models.RunAction{
								Path: "ls",
							},
						}
					}
				})

				It("deletes the container", func() {
					Ω(executorClient.DeleteContainerCallCount()).Should(Equal(1))
					Ω(executorClient.DeleteContainerArgsForCall(0)).Should(Equal("some-task-guid"))
				})
			})

			Context("which belongs to a resolving task", func() {
				BeforeEach(func() {
					snapshot.GetTaskStub = func(taskGuid string) *models.Task {
						Ω(taskGuid).Should(Equal("some-task-guid"))
						return &models.Task{
							State: models.TaskStateResolving,
							Action: &models.RunAction{
								Path: "ls",
							},
						}
					}
				})

				It("deletes the container", func() {
					Ω(executorClient.DeleteContainerCallCount()).Should(Equal(1))
					Ω(executorClient.DeleteContainerArgsForCall(0)).Should(Equal("some-task-guid"))
				})
			})

			Context("for which there is no associated task", func() {
				BeforeEach(func() {
					snapshot.GetTaskStub = func(taskGuid string) *models.Task {
						Ω(taskGuid).Should(Equal("some-task-guid"))
						return nil
					}
				})

				It("deletes the container", func() {
					Ω(executorClient.DeleteContainerCallCount()).Should(Equal(1))
					Ω(executorClient.DeleteContainerArgsForCall(0)).Should(Equal("some-task-guid"))
				})
			})
		})
	})
})
