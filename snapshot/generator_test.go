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

	Describe("OperationStream", func() {
		var (
			stream    <-chan operationq.Operation
			streamErr error
		)

		JustBeforeEach(func() {
			stream, streamErr = generator.OperationStream(logger)
		})

		Context("when subscribing to the executor succeeds", func() {
			var receivedEvents chan<- executor.Event

			BeforeEach(func() {
				events := make(chan executor.Event, 1)
				receivedEvents = events

				fakeExecutorClient.SubscribeToEventsReturns(events, nil)
			})

			Context("when a container lifecycle event appears", func() {
				var container executor.Container

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
					receivedEvents <- executor.NewContainerCompleteEvent(container)
					close(receivedEvents)
				})

				Context("when the lifecycle is LRP", func() {
					BeforeEach(func() {
						container.Tags[rep.LifecycleTag] = rep.LRPLifecycle
					})

					It("yields an operation", func() {
						Eventually(stream).Should(Receive())
					})

					Describe("the operation", func() {
						It("is keyed by the process guid", func() {
							var operation operationq.Operation
							Eventually(stream).Should(Receive(&operation))
							Ω(operation.Key()).Should(Equal("some-process-guid"))
						})

						It("processes using the lrp processor", func() {
							var operation operationq.Operation
							Eventually(stream).Should(Receive(&operation))

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

						It("yields an operation", func() {
							Eventually(stream).Should(Receive())
						})

						Describe("the operation", func() {
							It("has the task guid as its key", func() {
								var operation operationq.Operation
								Eventually(stream).Should(Receive(&operation))
								Ω(operation.Key()).Should(Equal("some-instance-guid"))
							})

							It("processes using the task processor", func() {
								var operation operationq.Operation
								Eventually(stream).Should(Receive(&operation))

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

						It("skips the event", func() {
							_, ok := <-stream
							Ω(ok).Should(BeFalse())
						})
					})
				})

				Context("when the lifecycle is Inconceivable", func() {
					BeforeEach(func() {
						container.Tags[rep.LifecycleTag] = "inconceivable"
					})

					It("skips the event", func() {
						_, ok := <-stream
						Ω(ok).Should(BeFalse())
					})
				})
			})
		})

		Context("when subscribing to the executor fails", func() {
			disaster := errors.New("nope")

			BeforeEach(func() {
				fakeExecutorClient.SubscribeToEventsReturns(nil, disaster)
			})

			It("returns the error", func() {
				Ω(streamErr).Should(Equal(disaster))
			})
		})
	})
})
