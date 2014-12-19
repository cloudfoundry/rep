package reaper_test

import (
	"errors"

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

	Describe("stopping containers which are not supposed to be running anymore", func() {
		It("queries task containers", func() {
			Ω(snapshot.ListContainersCallCount()).Should(Equal(1))

			Ω(snapshot.ListContainersArgsForCall(0)).To(Equal(executor.Tags{
				rep.LifecycleTag: rep.TaskLifecycle,
			}))
		})

		Context("when there is a reserved container", func() {
			BeforeEach(func() {
				snapshot.ListContainersReturns([]executor.Container{
					executor.Container{Guid: "some-task-guid", State: executor.StateReserved},
				})

				snapshot.LookupTaskReturns(nil, false, nil)
			})

			It("does not stop the container", func() {
				Ω(executorClient.StopContainerCallCount()).Should(Equal(0))
			})
		})

		Context("when there is a created container", func() {
			BeforeEach(func() {
				snapshot.ListContainersReturns([]executor.Container{
					executor.Container{Guid: "some-task-guid", State: executor.StateCreated},
				})
			})

			Context("which belongs to a pending task", func() {
				BeforeEach(func() {
					snapshot.LookupTaskStub = func(taskGuid string) (*models.Task, bool, error) {
						Ω(taskGuid).Should(Equal("some-task-guid"))
						return &models.Task{State: models.TaskStatePending}, true, nil
					}
				})

				It("does not stop the container", func() {
					Ω(executorClient.StopContainerCallCount()).Should(Equal(0))
				})
			})

			Context("which belongs to a completed task", func() {
				BeforeEach(func() {
					snapshot.LookupTaskStub = func(taskGuid string) (*models.Task, bool, error) {
						Ω(taskGuid).Should(Equal("some-task-guid"))
						return &models.Task{
							State: models.TaskStateCompleted,
							Action: &models.RunAction{
								Path: "ls",
							},
						}, true, nil
					}
				})

				It("stops the container", func() {
					Ω(executorClient.StopContainerCallCount()).Should(Equal(1))
					Ω(executorClient.StopContainerArgsForCall(0)).Should(Equal("some-task-guid"))
				})
			})

			Context("which belongs to a resolving task", func() {
				BeforeEach(func() {
					snapshot.LookupTaskStub = func(taskGuid string) (*models.Task, bool, error) {
						Ω(taskGuid).Should(Equal("some-task-guid"))
						return &models.Task{
							State: models.TaskStateResolving,
							Action: &models.RunAction{
								Path: "ls",
							},
						}, true, nil
					}
				})

				It("stops the container", func() {
					Ω(executorClient.StopContainerCallCount()).Should(Equal(1))
					Ω(executorClient.StopContainerArgsForCall(0)).Should(Equal("some-task-guid"))
				})
			})

			Context("for which there is no associated task", func() {
				BeforeEach(func() {
					snapshot.LookupTaskStub = func(taskGuid string) (*models.Task, bool, error) {
						Ω(taskGuid).Should(Equal("some-task-guid"))
						return nil, false, nil
					}
				})

				It("stops the container", func() {
					Ω(executorClient.StopContainerCallCount()).Should(Equal(1))
					Ω(executorClient.StopContainerArgsForCall(0)).Should(Equal("some-task-guid"))
				})
			})

			Context("when checking for the task fails", func() {
				BeforeEach(func() {
					snapshot.LookupTaskStub = func(taskGuid string) (*models.Task, bool, error) {
						Ω(taskGuid).Should(Equal("some-task-guid"))
						return nil, false, errors.New("welp")
					}
				})

				It("does not stop the container", func() {
					Ω(executorClient.StopContainerCallCount()).Should(Equal(0))
				})
			})
		})
	})
})
