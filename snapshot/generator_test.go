package snapshot_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/executor"
	efakes "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/snapshot"
	"github.com/cloudfoundry-incubator/rep/snapshot/fake_snapshot"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/pivotal-golang/operationq"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Generator", func() {
	var (
		logger             *lagertest.TestLogger
		cellID             string
		fakeExecutorClient *efakes.FakeClient
		fakeBBS            *fake_bbs.FakeRepBBS
		fakeProcessor      *fake_snapshot.FakeSnapshotProcessor

		generator snapshot.Generator
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test")
		cellID = "some-cell-id"
		fakeBBS = new(fake_bbs.FakeRepBBS)
		fakeExecutorClient = new(efakes.FakeClient)
		fakeProcessor = new(fake_snapshot.FakeSnapshotProcessor)

		generator = snapshot.NewGenerator(cellID, fakeBBS, fakeExecutorClient, fakeProcessor)
	})

	Describe("ContainerOperation", func() {
		var (
			container executor.Container

			operation operationq.Operation
			opErr     error
		)

		BeforeEach(func() {
			container = executor.Container{
				Guid: "some-instance-guid",

				State: executor.StateCompleted,

				Tags: executor.Tags{
					rep.ProcessGuidTag:  "some-process-guid",
					rep.DomainTag:       "some-domain",
					rep.ProcessIndexTag: "1",
				},
			}
		})

		JustBeforeEach(func() {
			operation, opErr = generator.ContainerOperation(logger, container)
		})

		Context("when the lifecycle is LRP", func() {
			BeforeEach(func() {
				container.Tags[rep.LifecycleTag] = rep.LRPLifecycle
			})

			It("succeeds", func() {
				Ω(opErr).ShouldNot(HaveOccurred())
			})

			Describe("the operation", func() {
				It("is keyed by the process guid", func() {
					Ω(operation.Key()).Should(Equal("some-process-guid"))
				})

				It("processes using the lrp processor", func() {
					operation.Execute()

					expectedLRPKey := models.NewActualLRPKey("some-process-guid", 1, "some-domain")
					expectedLRPContainerKey := models.NewActualLRPContainerKey("some-instance-guid", "some-cell-id")
					expectedSnapshotLRP := snapshot.NewLRP(expectedLRPKey, expectedLRPContainerKey)
					expectedSnapshot := snapshot.NewLRPSnapshot(expectedSnapshotLRP, &container)

					Ω(fakeProcessor.ProcessCallCount()).Should(Equal(1))
					processedLogger, processedSnapshot := fakeProcessor.ProcessArgsForCall(0)
					Ω(processedLogger).ShouldNot(BeNil())
					Ω(processedSnapshot).Should(Equal(expectedSnapshot))
				})
			})
		})

		Context("when the lifecycle is Task", func() {
			BeforeEach(func() {
				container.Tags[rep.LifecycleTag] = rep.TaskLifecycle
			})

			Context("when getting the task from the bbs succeeds", func() {
				var task models.Task

				BeforeEach(func() {
					task = models.Task{
						TaskGuid:   "some-instance-guid",
						State:      models.TaskStateRunning,
						ResultFile: "some-result-file",
					}

					fakeBBS.TaskByGuidReturns(task, nil)
				})

				It("succeeds", func() {
					Ω(opErr).ShouldNot(HaveOccurred())
				})

				Describe("the operation", func() {
					It("has the task guid as its key", func() {
						Ω(operation.Key()).Should(Equal("some-instance-guid"))
					})

					It("processes using the task processor", func() {
						operation.Execute()

						expectedSnapshotTask := snapshot.NewTask(task.TaskGuid, cellID, task.State, task.ResultFile)
						expectedSnapshot := snapshot.NewTaskSnapshot(expectedSnapshotTask, &container)

						Ω(fakeProcessor.ProcessCallCount()).Should(Equal(1))
						processedLogger, processedSnapshot := fakeProcessor.ProcessArgsForCall(0)
						Ω(processedLogger).ShouldNot(BeNil())
						Ω(processedSnapshot).Should(Equal(expectedSnapshot))
					})
				})
			})

			Context("when getting the task from the bbs fails", func() {
				disaster := errors.New("nope")

				BeforeEach(func() {
					fakeBBS.TaskByGuidReturns(models.Task{}, disaster)
				})

				It("fails", func() {
					Ω(opErr).Should(Equal(disaster))
				})
			})
		})

		Context("when the lifecycle is Inconceivable", func() {
			BeforeEach(func() {
				container.Tags[rep.LifecycleTag] = "inconceivable"
			})

			It("fails", func() {
				Ω(opErr).Should(HaveOccurred())
			})
		})
	})
})
