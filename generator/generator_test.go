package generator_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/executor"
	efakes "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/generator"
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
		cellID             string
		fakeExecutorClient *efakes.FakeClient

		opGenerator generator.Generator
	)

	BeforeEach(func() {
		cellID = "some-cell-id"
		fakeExecutorClient = new(efakes.FakeClient)

		opGenerator = generator.New(cellID, fakeBBS, fakeExecutorClient)
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

		It("logs its lifecycle", func() {
			Ω(logger).Should(Say(sessionPrefix + "subscribing"))
		})

		Context("when subscribing to the executor succeeds", func() {
			var receivedEvents chan<- executor.Event

			BeforeEach(func() {
				events := make(chan executor.Event, 1)
				receivedEvents = events

				fakeExecutorClient.SubscribeToEventsReturns(events, nil)
			})

			It("logs that it succeeded", func() {
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
