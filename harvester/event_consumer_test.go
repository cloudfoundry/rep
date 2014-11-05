package harvester_test

import (
	"errors"
	"os"

	"github.com/cloudfoundry-incubator/executor"
	efakes "github.com/cloudfoundry-incubator/executor/fakes"
	. "github.com/cloudfoundry-incubator/rep/harvester"
	"github.com/cloudfoundry-incubator/rep/harvester/fakes"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EventConsumer", func() {
	var (
		executorClient *efakes.FakeClient
		processor      *fakes.FakeProcessor

		consumer ifrit.Runner
		process  ifrit.Process
	)

	BeforeEach(func() {
		executorClient = new(efakes.FakeClient)
		processor = new(fakes.FakeProcessor)

		consumer = NewEventConsumer(executorClient, processor)
	})

	JustBeforeEach(func() {
		process = ifrit.Invoke(consumer)
	})

	AfterEach(func() {
		process.Signal(os.Interrupt)
		Eventually(process.Wait()).Should(Receive())
	})

	Context("when subscribing to events succeeds", func() {
		var (
			receivedEvents chan<- executor.Event
		)

		BeforeEach(func() {
			events := make(chan executor.Event)
			receivedEvents = events

			executorClient.SubscribeToEventsReturns(events, nil)
		})

		Context("when a complete container event arrives", func() {
			var completedContainer executor.Container

			BeforeEach(func() {
				completedContainer = executor.Container{
					Guid:  "first-completed-guid",
					State: executor.StateCompleted,
				}
			})

			JustBeforeEach(func() {
				receivedEvents <- executor.ContainerCompleteEvent{
					Container: completedContainer,
				}
			})

			Context("and it has no tags", func() {
				BeforeEach(func() {
					completedContainer.Tags = nil
				})

				It("does not process the container", func() {
					Consistently(processor.ProcessCallCount).Should(BeZero())
				})
			})

			Context("and its lifecycle is task", func() {
				BeforeEach(func() {
					completedContainer.Tags = executor.Tags{
						LifecycleTag: TaskLifecycle,
					}
				})

				It("processes the completed container", func() {
					Eventually(processor.ProcessCallCount).Should(Equal(1))
					Ω(processor.ProcessArgsForCall(0)).Should(Equal(completedContainer))
				})
			})

			Context("and its lifecycle is something else", func() {
				BeforeEach(func() {
					completedContainer.Tags = executor.Tags{
						LifecycleTag: "banana",
					}
				})

				It("does not process the container", func() {
					Consistently(processor.ProcessCallCount).Should(BeZero())
				})
			})
		})

		Context("when the event stream terminates", func() {
			JustBeforeEach(func() {
				close(receivedEvents)
			})

			It("exits with success", func() {
				Eventually(process.Wait()).Should(Receive(BeNil()))
			})
		})
	})

	Context("when subscribing to events fails", func() {
		disaster := errors.New("oh no!")

		BeforeEach(func() {
			executorClient.SubscribeToEventsReturns(nil, disaster)
		})

		It("exits with failure", func() {
			Eventually(process.Wait()).Should(Receive(Equal(disaster)))
		})
	})
})
