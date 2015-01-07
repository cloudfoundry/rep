package harmonizer_test

import (
	"errors"
	"os"
	"strconv"

	"github.com/cloudfoundry-incubator/executor"
	efakes "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	. "github.com/cloudfoundry-incubator/rep/harmonizer"
	"github.com/cloudfoundry-incubator/rep/snapshot/fake_snapshot"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/pivotal-golang/operationq/fake_operationq"
	"github.com/tedsuo/ifrit"
)

var _ = Describe("EventConsumer", func() {
	var (
		logger             *lagertest.TestLogger
		fakeExecutorClient *efakes.FakeClient
		fakeGenerator      *fake_snapshot.FakeGenerator
		fakeQueue          *fake_operationq.FakeQueue

		consumer *EventConsumer
		process  ifrit.Process
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test")
		fakeExecutorClient = new(efakes.FakeClient)
		fakeGenerator = new(fake_snapshot.FakeGenerator)
		fakeQueue = new(fake_operationq.FakeQueue)

		consumer = NewEventConsumer(logger, fakeExecutorClient, fakeGenerator, fakeQueue)
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

			fakeExecutorClient.SubscribeToEventsReturns(events, nil)
		})

		Context("when a container complete event is received", func() {
			var container executor.Container

			actualLRPKey := models.NewActualLRPKey("some-process-guid", 1, "some-domain")
			containerKey := models.NewActualLRPContainerKey("some-instance-guid", "some-cell-id")

			BeforeEach(func() {
				container = executor.Container{
					Guid: containerKey.InstanceGuid,

					State: executor.StateCompleted,

					Tags: executor.Tags{
						rep.ProcessGuidTag:  actualLRPKey.ProcessGuid,
						rep.DomainTag:       actualLRPKey.Domain,
						rep.LifecycleTag:    rep.LRPLifecycle,
						rep.ProcessIndexTag: strconv.Itoa(actualLRPKey.Index),
					},
				}
			})

			Context("when the generator yields an operation for the container", func() {
				var fakeOperation *fake_operationq.FakeOperation

				BeforeEach(func() {
					fakeOperation = new(fake_operationq.FakeOperation)
					fakeGenerator.ContainerOperationReturns(fakeOperation, nil)
				})

				It("pushes it onto the queue", func() {
					receivedEvents <- executor.NewContainerCompleteEvent(container)

					Eventually(fakeQueue.PushCallCount).Should(Equal(1))
					Ω(fakeQueue.PushArgsForCall(0)).Should(Equal(fakeOperation))
				})
			})

			Context("when the generator fails to generate an operation", func() {
				disaster := errors.New("nope")

				BeforeEach(func() {
					fakeGenerator.ContainerOperationReturns(nil, disaster)
				})

				It("logs", func() {
					receivedEvents <- executor.NewContainerCompleteEvent(container)

					Eventually(logger).Should(gbytes.Say("failed-to-generate-operation"))
					Eventually(logger).Should(gbytes.Say("nope"))
				})
			})
		})
	})

	Context("when subscribing to events fails", func() {
		disaster := errors.New("nope")

		BeforeEach(func() {
			fakeExecutorClient.SubscribeToEventsReturns(nil, disaster)
		})

		It("exits with failure", func() {
			Eventually(process.Wait()).Should(Receive(Equal(disaster)))
		})
	})
})
