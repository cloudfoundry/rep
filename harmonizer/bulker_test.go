package harmonizer_test

import (
	"errors"
	"os"
	"time"

	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context"
	"github.com/cloudfoundry-incubator/rep/generator/fake_generator"
	"github.com/cloudfoundry-incubator/rep/harmonizer"
	"github.com/cloudfoundry/dropsonde/metric_sender/fake"
	"github.com/cloudfoundry/dropsonde/metrics"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/pivotal-golang/clock/fakeclock"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/pivotal-golang/operationq"
	"github.com/pivotal-golang/operationq/fake_operationq"
	"github.com/tedsuo/ifrit"
)

var _ = Describe("Bulker", func() {
	var (
		sender *fake.FakeMetricSender

		logger                 *lagertest.TestLogger
		pollInterval           time.Duration
		evacuationPollInterval time.Duration
		fakeClock              *fakeclock.FakeClock
		fakeGenerator          *fake_generator.FakeGenerator
		fakeQueue              *fake_operationq.FakeQueue
		evacuatable            evacuation_context.Evacuatable
		evacuationNotifier     evacuation_context.EvacuationNotifier

		bulker  *harmonizer.Bulker
		process ifrit.Process
	)

	BeforeEach(func() {
		sender = fake.NewFakeMetricSender()
		metrics.Initialize(sender, nil)

		logger = lagertest.NewTestLogger("test")
		pollInterval = 30 * time.Second
		evacuationPollInterval = 10 * time.Second
		fakeClock = fakeclock.NewFakeClock(time.Unix(123, 456))
		fakeGenerator = new(fake_generator.FakeGenerator)
		fakeQueue = new(fake_operationq.FakeQueue)

		evacuatable, _, evacuationNotifier = evacuation_context.New()

		bulker = harmonizer.NewBulker(logger, pollInterval, evacuationPollInterval, evacuationNotifier, fakeClock, fakeGenerator, fakeQueue)
	})

	JustBeforeEach(func() {
		process = ifrit.Invoke(bulker)
		Eventually(fakeClock.WatcherCount).Should(Equal(1))
	})

	AfterEach(func() {
		process.Signal(os.Interrupt)
		Eventually(process.Wait()).Should(Receive())
	})

	itPerformsBatchOperations := func() {
		Context("when generating the batch operations succeeds", func() {
			var (
				operation1 *fake_operationq.FakeOperation
				operation2 *fake_operationq.FakeOperation
			)

			BeforeEach(func() {
				operation1 = new(fake_operationq.FakeOperation)
				operation2 = new(fake_operationq.FakeOperation)

				fakeGenerator.BatchOperationsStub = func(lager.Logger) (map[string]operationq.Operation, error) {
					fakeClock.Increment(10 * time.Second)
					return map[string]operationq.Operation{"guid1": operation1, "guid2": operation2}, nil
				}
			})

			It("pushes them onto the queue", func() {
				Eventually(fakeQueue.PushCallCount).Should(Equal(2))

				enqueuedOperations := make([]operationq.Operation, 0, 2)
				enqueuedOperations = append(enqueuedOperations, fakeQueue.PushArgsForCall(0))
				enqueuedOperations = append(enqueuedOperations, fakeQueue.PushArgsForCall(1))

				Expect(enqueuedOperations).To(ConsistOf(operation1, operation2))
			})

			It("emits the duration it took to generate the batch operations", func() {
				Eventually(fakeQueue.PushCallCount).Should(Equal(2))

				reportedDuration := sender.GetValue("RepBulkSyncDuration")
				Expect(reportedDuration.Unit).To(Equal("nanos"))
				Expect(reportedDuration.Value).To(BeNumerically("==", 10*time.Second))
			})
		})

		Context("when generating the batch operations fails", func() {
			disaster := errors.New("nope")

			BeforeEach(func() {
				fakeGenerator.BatchOperationsReturns(nil, disaster)
			})

			It("logs the error", func() {
				Eventually(logger).Should(gbytes.Say("failed-to-generate-operations"))
				Eventually(logger).Should(gbytes.Say("nope"))
			})
		})
	}

	Context("when the poll interval elapses", func() {
		JustBeforeEach(func() {
			fakeClock.Increment(pollInterval + 1)
		})

		itPerformsBatchOperations()

		Context("and elapses again", func() {
			BeforeEach(func() {
				fakeClock.Increment(pollInterval)
			})

			itPerformsBatchOperations()
		})
	})

	Context("when the poll interval has not elapsed", func() {
		JustBeforeEach(func() {
			fakeClock.Increment(pollInterval - 1)
		})

		It("does not fetch batch operations", func() {
			Consistently(fakeGenerator.BatchOperationsCallCount).Should(BeZero())
		})
	})

	Context("when evacuation starts", func() {
		BeforeEach(func() {
			evacuatable.Evacuate()
		})

		itPerformsBatchOperations()

		It("batches operations only once", func() {
			Eventually(fakeGenerator.BatchOperationsCallCount).Should(Equal(1))
			Consistently(fakeGenerator.BatchOperationsCallCount).Should(Equal(1))
		})

		Context("when the evacuation interval elapses", func() {
			It("batches operations again", func() {
				Eventually(fakeGenerator.BatchOperationsCallCount).Should(Equal(1))
				fakeClock.Increment(evacuationPollInterval + time.Second)
				Eventually(fakeGenerator.BatchOperationsCallCount).Should(Equal(2))
				Consistently(fakeGenerator.BatchOperationsCallCount).Should(Equal(2))
			})
		})
	})
})
