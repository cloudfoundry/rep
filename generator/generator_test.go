package generator_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/executor"
	efakes "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/generator"
	"github.com/cloudfoundry-incubator/rep/generator/internal/fake_internal"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/operationq"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
)

type BogusEvent struct{}

func (BogusEvent) EventType() executor.EventType {
	return executor.EventTypeInvalid
}

var _ = Describe("Generator", func() {
	var (
		cellID                string
		fakeExecutorClient    *efakes.FakeClient
		fakeLRPProcessor      *fake_internal.FakeLRPProcessor
		fakeTaskProcessor     *fake_internal.FakeTaskProcessor
		fakeContainerDelegate *fake_internal.FakeContainerDelegate

		opGenerator generator.Generator
	)

	BeforeEach(func() {
		cellID = "some-cell-id"
		fakeExecutorClient = new(efakes.FakeClient)
		fakeLRPProcessor = &fake_internal.FakeLRPProcessor{}
		fakeTaskProcessor = &fake_internal.FakeTaskProcessor{}
		fakeContainerDelegate = &fake_internal.FakeContainerDelegate{}

		opGenerator = generator.New(cellID, fakeBBS, fakeExecutorClient, fakeLRPProcessor, fakeTaskProcessor, fakeContainerDelegate)
	})

	Describe("BatchOperations", func() {
		const sessionName = "test.batch-operations"

		var (
			batch    map[string]operationq.Operation
			batchErr error
		)

		JustBeforeEach(func() {
			batch, batchErr = opGenerator.BatchOperations(logger)
		})

		It("logs its lifecycle", func() {
			Ω(logger).Should(Say(sessionName + ".started"))
		})

		It("retrieves all actual lrps for its cell id", func() {
			Ω(fakeBBS.ActualLRPGroupsByCellIDCallCount()).Should(Equal(1))
			Ω(fakeBBS.ActualLRPGroupsByCellIDArgsForCall(0)).Should(Equal(cellID))
		})

		It("retrieves all tasks for its cell id", func() {
			Ω(fakeBBS.TasksByCellIDCallCount()).Should(Equal(1))
			actualLogger, actualCellID := fakeBBS.TasksByCellIDArgsForCall(0)
			Ω(actualLogger.SessionName()).Should(Equal(sessionName))
			Ω(actualCellID).Should(Equal(cellID))
		})

		It("lists all containers from the executor", func() {
			Ω(fakeExecutorClient.ListContainersCallCount()).Should(Equal(1))
			tags := fakeExecutorClient.ListContainersArgsForCall(0)
			Ω(tags).Should(BeNil())
		})

		Context("when retrieving container and BBS data succeeds", func() {
			const (
				guidContainerOnly                 = "guid-container-only"
				guidContainerForInstanceLRP       = "guid-container-for-instance-lrp"
				guidContainerForEvacuatingLRP     = "guid-container-for-evacuating-lrp"
				guidContainerForTask              = "guid-container-for-task"
				guidInstanceLRPOnly               = "guid-instance-lrp-only"
				guidEvacuatingLRPOnly             = "guid-evacuating-lrp-only"
				guidInstanceAndEvacuatingLRPsOnly = "guid-instance-and-evacuating-lrps-only"
				guidTaskOnly                      = "guid-task-only"
			)

			BeforeEach(func() {
				containers := []executor.Container{
					{Guid: guidContainerOnly},
					{Guid: guidContainerForInstanceLRP},
					{Guid: guidContainerForEvacuatingLRP},
					{Guid: guidContainerForTask},
				}

				containerOnlyLRP := models.ActualLRP{ActualLRPContainerKey: models.NewActualLRPContainerKey(guidContainerForInstanceLRP, cellID)}
				instanceOnlyLRP := models.ActualLRP{ActualLRPContainerKey: models.NewActualLRPContainerKey(guidInstanceLRPOnly, cellID)}

				containerForEvacuatingLRP := models.ActualLRP{ActualLRPContainerKey: models.NewActualLRPContainerKey(guidContainerForEvacuatingLRP, cellID)}
				evacuatingOnlyLRP := models.ActualLRP{ActualLRPContainerKey: models.NewActualLRPContainerKey(guidEvacuatingLRPOnly, cellID)}

				instanceAndEvacuatingLRP := models.ActualLRP{ActualLRPContainerKey: models.NewActualLRPContainerKey(guidInstanceAndEvacuatingLRPsOnly, cellID)}

				lrpGroups := []models.ActualLRPGroup{
					{Instance: &containerOnlyLRP, Evacuating: nil},
					{Instance: &instanceOnlyLRP, Evacuating: nil},
					{Instance: &instanceAndEvacuatingLRP, Evacuating: &instanceAndEvacuatingLRP},
					{Instance: nil, Evacuating: &containerForEvacuatingLRP},
					{Instance: nil, Evacuating: &evacuatingOnlyLRP},
				}

				tasks := []models.Task{
					{TaskGuid: guidContainerForTask},
					{TaskGuid: guidTaskOnly},
				}

				fakeExecutorClient.ListContainersReturns(containers, nil)

				fakeBBS.ActualLRPGroupsByCellIDReturns(lrpGroups, nil)
				fakeBBS.TasksByCellIDReturns(tasks, nil)
			})

			It("does not return an error", func() {
				Ω(batchErr).ShouldNot(HaveOccurred())
			})

			It("logs success", func() {
				Ω(logger).Should(Say(sessionName + ".succeeded"))
			})

			It("returns a batch of the correct size", func() {
				Ω(batch).Should(HaveLen(8))
			})

			batchHasAContainerOperationForGuid := func(guid string, batch map[string]operationq.Operation) {
				Ω(batch).Should(HaveKey(guid))
				Ω(batch[guid]).Should(BeAssignableToTypeOf(new(generator.ContainerOperation)))
			}

			It("returns a container operation for a container with an instance lrp", func() {
				batchHasAContainerOperationForGuid(guidContainerForInstanceLRP, batch)
			})

			It("returns a container operation for a container with an evacuating lrp", func() {
				batchHasAContainerOperationForGuid(guidContainerForEvacuatingLRP, batch)
			})

			It("returns a container operation for a container with a task", func() {
				batchHasAContainerOperationForGuid(guidContainerForTask, batch)
			})

			It("returns a container operation for a container with nothing in bbs", func() {
				batchHasAContainerOperationForGuid(guidContainerOnly, batch)
			})

			It("returns a residual instance lrp operation for a guid with an instance lrp but no container", func() {
				guid := guidInstanceLRPOnly
				Ω(batch).Should(HaveKey(guid))
				Ω(batch[guid]).Should(BeAssignableToTypeOf(new(generator.ResidualInstanceLRPOperation)))
			})

			It("returns a residual evacuating lrp operation for a guid with an evacuating lrp but no container", func() {
				guid := guidEvacuatingLRPOnly
				Ω(batch).Should(HaveKey(guid))
				Ω(batch[guid]).Should(BeAssignableToTypeOf(new(generator.ResidualEvacuatingLRPOperation)))
			})

			It("returns a residual joint lrp operation for a guid with both an instance and an evacuating lrp but no container", func() {
				guid := guidInstanceAndEvacuatingLRPsOnly
				Ω(batch).Should(HaveKey(guid))
				Ω(batch[guid]).Should(BeAssignableToTypeOf(new(generator.ResidualJointLRPOperation)))
			})

			It("returns a residual task operation for a task with no container", func() {
				guid := guidTaskOnly
				Ω(batch).Should(HaveKey(guid))
				Ω(batch[guid]).Should(BeAssignableToTypeOf(new(generator.ResidualTaskOperation)))
			})

		})

		Context("when retrieving data fails", func() {
			Context("when retrieving the containers fails", func() {
				BeforeEach(func() {
					fakeExecutorClient.ListContainersReturns(nil, errors.New("oh no, no container!"))
				})

				It("returns an error", func() {
					Ω(batchErr).Should(HaveOccurred())
					Ω(batchErr).Should(MatchError(ContainSubstring("oh no, no container!")))
				})

				It("logs the failure", func() {
					Ω(logger).Should(Say(sessionName + ".failed-to-list-containers"))
				})
			})

			Context("when retrieving the tasks fails", func() {
				BeforeEach(func() {
					fakeBBS.TasksByCellIDReturns(nil, errors.New("oh no, no task!"))
				})

				It("returns an error", func() {
					Ω(batchErr).Should(HaveOccurred())
					Ω(batchErr).Should(MatchError(ContainSubstring("oh no, no task!")))
				})

				It("logs the failure", func() {
					Ω(logger).Should(Say(sessionName + ".failed-to-retrieve-tasks"))
				})
			})

			Context("when retrieving the LRP groups fails", func() {
				BeforeEach(func() {
					fakeBBS.ActualLRPGroupsByCellIDReturns(nil, errors.New("oh no, no lrp!"))
				})

				It("returns an error", func() {
					Ω(batchErr).Should(HaveOccurred())
					Ω(batchErr).Should(MatchError(ContainSubstring("oh no, no lrp!")))
				})

				It("logs the failure", func() {
					Ω(logger).Should(Say(sessionName + ".failed-to-retrieve-lrp-groups"))
				})
			})
		})
	})

	Describe("OperationStream", func() {
		const sessionPrefix = "test.operation-stream."

		var (
			stream    <-chan operationq.Operation
			streamErr error
		)

		JustBeforeEach(func() {
			stream, streamErr = opGenerator.OperationStream(logger)
		})

		Context("when subscribing to the executor succeeds", func() {
			var receivedEvents chan<- executor.Event

			BeforeEach(func() {
				events := make(chan executor.Event, 1)
				receivedEvents = events

				fakeExecutorSource := new(efakes.FakeEventSource)
				fakeExecutorSource.NextStub = func() (executor.Event, error) {
					ev, ok := <-events
					if !ok {
						return nil, errors.New("nope")
					}

					return ev, nil
				}

				fakeExecutorClient.SubscribeToEventsReturns(fakeExecutorSource, nil)
			})

			It("logs that it succeeded", func() {
				Ω(logger).Should(Say(sessionPrefix + "subscribing"))
				Ω(logger).Should(Say(sessionPrefix + "succeeded-subscribing"))
			})

			Context("when the event stream closes", func() {
				BeforeEach(func() {
					close(receivedEvents)
				})

				It("closes the operation stream", func() {
					Eventually(stream).Should(BeClosed())
				})

				It("logs the closure", func() {
					Eventually(logger).Should(Say(sessionPrefix + "event-stream-closed"))
				})
			})

			Context("when an executor event appears", func() {
				AfterEach(func() {
					close(receivedEvents)
				})

				Context("when the event is a lifecycle event", func() {
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
					})

					Context("when the lifecycle is LRP", func() {
						BeforeEach(func() {
							container.Tags[rep.LifecycleTag] = rep.LRPLifecycle
						})

						It("yields an operation for that container", func() {
							var operation operationq.Operation
							Eventually(stream).Should(Receive(&operation))
							Ω(operation.Key()).Should(Equal(container.Guid))
						})
					})

					Context("when the lifecycle is Task", func() {
						var task models.Task

						BeforeEach(func() {
							container.Tags[rep.LifecycleTag] = rep.TaskLifecycle

							task = models.Task{
								TaskGuid:   "some-instance-guid",
								State:      models.TaskStateRunning,
								ResultFile: "some-result-file",
							}

							fakeBBS.TaskByGuidReturns(task, nil)
						})

						It("yields an operation for that container", func() {
							var operation operationq.Operation
							Eventually(stream).Should(Receive(&operation))
							Ω(operation.Key()).Should(Equal(container.Guid))
						})
					})
				})

				Context("when the event is not a lifecycle event", func() {
					BeforeEach(func() {
						receivedEvents <- BogusEvent{}
					})

					It("does not yield an operation", func() {
						Consistently(stream).ShouldNot(Receive())
					})

					It("logs the non-lifecycle event", func() {
						Eventually(logger).Should(Say(sessionPrefix + "received-non-lifecycle-event"))
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

			It("logs the failure", func() {
				Ω(logger).Should(Say(sessionPrefix + "failed-subscribing"))
			})
		})
	})
})
