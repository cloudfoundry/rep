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
			Ω(fakeBBS.ActualLRPsByCellIDCallCount()).Should(Equal(1))
			Ω(fakeBBS.ActualLRPsByCellIDArgsForCall(0)).Should(Equal(cellID))
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
			var (
				instanceGuid1 string
				instanceGuid2 string
				instanceGuid3 string
				instanceGuid4 string
				instanceGuid5 string
			)

			BeforeEach(func() {
				instanceGuid1 = "instance-guid-1"
				instanceGuid2 = "instance-guid-2"
				instanceGuid3 = "instance-guid-3"
				instanceGuid4 = "instance-guid-4"
				instanceGuid5 = "instance-guid-5"

				containers := []executor.Container{
					{Guid: instanceGuid1},
					{Guid: instanceGuid2},
					{Guid: instanceGuid3},
				}

				lrps := []models.ActualLRP{
					{ActualLRPContainerKey: models.NewActualLRPContainerKey(instanceGuid1, cellID)},
					{ActualLRPContainerKey: models.NewActualLRPContainerKey(instanceGuid4, cellID)},
				}

				tasks := []models.Task{
					{TaskGuid: instanceGuid2},
					{TaskGuid: instanceGuid5},
				}

				fakeExecutorClient.ListContainersReturns(containers, nil)

				fakeBBS.ActualLRPsByCellIDReturns(lrps, nil)
				fakeBBS.TasksByCellIDReturns(tasks, nil)
			})

			It("does not return an error", func() {
				Ω(batchErr).ShouldNot(HaveOccurred())
			})

			It("logs success", func() {
				Ω(logger).Should(Say(sessionName + ".succeeded"))
			})

			It("returns a batch of the correct size", func() {
				Ω(batch).Should(HaveLen(5))
			})

			assertBatchHasAContainerOperationForGuid := func(guid string, batch map[string]operationq.Operation) {
				operation, found := batch[guid]
				Ω(found).Should(BeTrue(), "no operation for '"+guid+"'")
				_, ok := operation.(*generator.ContainerOperation)
				Ω(ok).Should(BeTrue(), "operation for '"+guid+"' was not a container operation")
			}

			It("returns a container operation for a container with an lrp", func() {
				assertBatchHasAContainerOperationForGuid(instanceGuid1, batch)
			})

			It("returns a container operation for a container with a task", func() {
				assertBatchHasAContainerOperationForGuid(instanceGuid2, batch)
			})

			It("returns a container operation for a container with nothing in bbs", func() {
				assertBatchHasAContainerOperationForGuid(instanceGuid3, batch)
			})

			It("returns a missing lrp operation for an lrp with no container", func() {
				guid := instanceGuid4
				operation, found := batch[guid]
				Ω(found).Should(BeTrue(), "no operation for '"+guid+"'")
				_, ok := operation.(*generator.MissingLRPOperation)
				Ω(ok).Should(BeTrue(), "operation for '"+guid+"' was not a missing lrp operation")
			})

			It("returns a missing task operation for a task with no container", func() {
				guid := instanceGuid5
				operation, found := batch[guid]
				Ω(found).Should(BeTrue(), "no operation for '"+guid+"'")
				_, ok := operation.(*generator.MissingTaskOperation)
				Ω(ok).Should(BeTrue(), "operation for '"+guid+"' was not a missing task operation")
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

			Context("when retrieving the LRPs fails", func() {
				BeforeEach(func() {
					fakeBBS.ActualLRPsByCellIDReturns(nil, errors.New("oh no, no lrp!"))
				})

				It("returns an error", func() {
					Ω(batchErr).Should(HaveOccurred())
					Ω(batchErr).Should(MatchError(ContainSubstring("oh no, no lrp!")))
				})

				It("logs the failure", func() {
					Ω(logger).Should(Say(sessionName + ".failed-to-retrieve-lrps"))
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
