package gatherer_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	. "github.com/cloudfoundry-incubator/rep/gatherer"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/bbserrors"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Snapshot", func() {
	var snapshot Snapshot
	var snapshoterr error
	var bbs *fake_bbs.FakeRepBBS
	var executorClient *fakes.FakeClient

	var activeContainers []executor.Container
	var reservedContainers []executor.Container
	var actualLRPs []models.ActualLRP
	var tasks []models.Task

	var lrpTags = executor.Tags{rep.LifecycleTag: rep.LRPLifecycle}
	var taskTags = executor.Tags{rep.LifecycleTag: rep.TaskLifecycle}

	BeforeEach(func() {
		bbs = new(fake_bbs.FakeRepBBS)
		executorClient = new(fakes.FakeClient)

		activeContainers = []executor.Container{
			{Guid: "first-completed-task-guid", State: executor.StateCompleted, Tags: taskTags},
			{Guid: "second-completed-task-guid", State: executor.StateCompleted, Tags: taskTags},
			{Guid: "first-completed-lrp-guid", State: executor.StateCompleted, Tags: lrpTags},
			{Guid: "first-completed-foobar-guid", State: executor.StateCompleted, Tags: executor.Tags{rep.LifecycleTag: "foobar"}},
			{Guid: "created-task-guid", State: executor.StateCreated, Tags: taskTags},
			{Guid: "initializing-task-guid", State: executor.StateInitializing, Tags: taskTags},
		}

		reservedContainers = []executor.Container{
			{Guid: "reserved-task-guid", State: executor.StateReserved, Tags: taskTags},
		}

		executorClient.ListContainersReturns(append(activeContainers, reservedContainers...), nil)

		actualLRPs = []models.ActualLRP{
			models.ActualLRP{
				InstanceGuid: "instance-guid-1",
			},
			models.ActualLRP{
				InstanceGuid: "instance-guid-2",
			},
		}

		bbs.ActualLRPsByCellIDReturns(actualLRPs, nil)

		tasks = []models.Task{
			models.Task{
				TaskGuid: "task-guid-2",
				State:    models.TaskStateRunning,
				Action: &models.RunAction{
					Path: "ls",
				},
			},
		}

		bbs.TasksByCellIDReturns(tasks, nil)

		// set base case for LookupTask fallthrough
		bbs.TaskByGuidReturns(nil, bbserrors.ErrStoreResourceNotFound)
	})

	JustBeforeEach(func() {
		snapshot, snapshoterr = NewSnapshot("cell-1", bbs, executorClient)
	})

	Describe("ListContainers", func() {
		Context("no tags", func() {
			It("returns all the active containers", func() {
				c := snapshot.ListContainers(nil)
				Ω(c).Should(ConsistOf(append(activeContainers, reservedContainers...)))
			})
		})

		Context("with tags", func() {
			It("returns the matched containers", func() {
				c := snapshot.ListContainers(lrpTags)
				Ω(c).Should(Equal([]executor.Container{
					{Guid: "first-completed-lrp-guid", State: executor.StateCompleted, Tags: lrpTags},
				}))
			})

			It("returns nothing when no matches", func() {
				c := snapshot.ListContainers(executor.Tags{rep.LifecycleTag: "bogus"})
				Ω(c).Should(HaveLen(0))
			})
		})
	})

	Describe("GetContainer", func() {
		It("returns the matched containers", func() {
			c, ok := snapshot.GetContainer("first-completed-task-guid")
			Ω(ok).Should(BeTrue())
			Ω(c).Should(Equal(&executor.Container{
				Guid:  "first-completed-task-guid",
				State: executor.StateCompleted,
				Tags:  taskTags,
			}))
		})

		It("returns nothing when its not there", func() {
			c, ok := snapshot.GetContainer("bogus")
			Ω(ok).Should(BeFalse())
			Ω(c).Should(BeNil())
		})
	})

	Describe("ActualLRPs", func() {
		It("returns all the actual LRPs", func() {
			a := snapshot.ActualLRPs()
			Ω(a).Should(Equal(actualLRPs))
		})
	})

	Describe("Tasks", func() {
		It("returns all the containers", func() {
			t := snapshot.Tasks()
			Ω(t).Should(Equal(tasks))
		})
	})

	Describe("LookupTask", func() {
		Context("when the task exists in the loaded set", func() {
			It("returns the matched task", func() {
				t, ok, err := snapshot.LookupTask("task-guid-2")
				Ω(err).ShouldNot(HaveOccurred())
				Ω(ok).Should(BeTrue())
				Ω(t).Should(Equal(&models.Task{
					TaskGuid: "task-guid-2",
					State:    models.TaskStateRunning,
					Action: &models.RunAction{
						Path: "ls",
					},
				},
				))
			})
		})

		Context("when the task does not exist in the loaded set", func() {
			It("checks for it by process guid", func() {
				t, ok, err := snapshot.LookupTask("bogus")
				Ω(err).ShouldNot(HaveOccurred())
				Ω(ok).Should(BeFalse())
				Ω(t).Should(BeNil())

				Ω(bbs.TaskByGuidCallCount()).Should(Equal(1))
				Ω(bbs.TaskByGuidArgsForCall(0)).Should(Equal("bogus"))
			})

			Context("and it actually does exist in the BBS, the cache was just old", func() {
				var foundTask *models.Task

				BeforeEach(func() {
					foundTask = &models.Task{}
					bbs.TaskByGuidReturns(foundTask, nil)
				})

				Context("and it's for the current cell", func() {
					BeforeEach(func() {
						foundTask.CellID = "cell-1"
					})

					It("returns the task", func() {
						t, ok, err := snapshot.LookupTask("some-guid")
						Ω(err).ShouldNot(HaveOccurred())
						Ω(ok).Should(BeTrue())
						Ω(t).Should(Equal(foundTask))
					})
				})

				Context("but it's for a different cell", func() {
					BeforeEach(func() {
						foundTask.CellID = "other-cell"
					})

					It("returns nothing", func() {
						t, ok, err := snapshot.LookupTask("some-guid")
						Ω(err).ShouldNot(HaveOccurred())
						Ω(ok).Should(BeFalse())
						Ω(t).Should(BeNil())
					})
				})
			})

			Context("and it also does not exist in the BBS", func() {
				BeforeEach(func() {
					bbs.TaskByGuidReturns(nil, bbserrors.ErrStoreResourceNotFound)
				})

				It("returns nothing", func() {
					t, ok, err := snapshot.LookupTask("bogus")
					Ω(err).ShouldNot(HaveOccurred())
					Ω(ok).Should(BeFalse())
					Ω(t).Should(BeNil())
				})
			})

			Context("and getting it from the BBS fails for some reason", func() {
				disaster := errors.New("some reason")

				BeforeEach(func() {
					bbs.TaskByGuidReturns(nil, disaster)
				})

				It("returns the error", func() {
					t, ok, err := snapshot.LookupTask("bogus")
					Ω(err).Should(Equal(disaster))
					Ω(ok).Should(BeFalse())
					Ω(t).Should(BeNil())
				})
			})
		})
	})

	Describe("NewSnapshot", func() {
		Context("with no error", func() {
			It("returns a snapshot", func() {
				Ω(snapshot).ShouldNot(BeNil())
				Ω(snapshoterr).ShouldNot(HaveOccurred())
			})
		})

		Context("when an error occurrs", func() {
			Describe("ListContainers", func() {
				BeforeEach(func() {
					executorClient.ListContainersReturns(nil, errors.New("uh oh"))
				})

				It("returns an error", func() {
					Ω(snapshoterr).Should(MatchError(ContainSubstring("uh oh")))
				})
			})

			Describe("ActualLRPsByCellID", func() {
				BeforeEach(func() {
					bbs.ActualLRPsByCellIDReturns(nil, errors.New("uh oh"))
				})

				It("returns an error", func() {
					Ω(snapshoterr).Should(MatchError(ContainSubstring("uh oh")))
				})
			})

			Describe("TasksByCellID", func() {
				BeforeEach(func() {
					bbs.TasksByCellIDReturns(nil, errors.New("uh oh"))
				})

				It("returns an error", func() {
					Ω(snapshoterr).Should(MatchError(ContainSubstring("uh oh")))
				})
			})

		})
	})
})
