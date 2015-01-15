package harmonizer_test

import (
	"errors"
	"os"
	"time"

	"github.com/cloudfoundry-incubator/rep/generator/fake_generator"
	"github.com/cloudfoundry-incubator/rep/harmonizer"
	"github.com/cloudfoundry/gunk/timeprovider/faketimeprovider"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/pivotal-golang/operationq"
	"github.com/pivotal-golang/operationq/fake_operationq"
	"github.com/tedsuo/ifrit"
)

var _ = Describe("Bulker", func() {
	var (
		logger           *lagertest.TestLogger
		pollInterval     time.Duration
		fakeTimeProvider *faketimeprovider.FakeTimeProvider
		fakeGenerator    *fake_generator.FakeGenerator
		fakeQueue        *fake_operationq.FakeQueue

		bulker  *harmonizer.Bulker
		process ifrit.Process
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test")
		pollInterval = 30 * time.Second
		fakeTimeProvider = faketimeprovider.New(time.Unix(123, 456))
		fakeGenerator = new(fake_generator.FakeGenerator)
		fakeQueue = new(fake_operationq.FakeQueue)

		bulker = harmonizer.NewBulker(logger, pollInterval, fakeTimeProvider, fakeGenerator, fakeQueue)
	})

	JustBeforeEach(func() {
		process = ifrit.Invoke(bulker)
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

				fakeGenerator.BatchOperationsReturns(map[string]operationq.Operation{"guid1": operation1, "guid2": operation2}, nil)
			})

			It("pushes them onto the queue", func() {
				Eventually(fakeQueue.PushCallCount).Should(Equal(2))

				enqueuedOperations := make([]operationq.Operation, 0, 2)
				enqueuedOperations = append(enqueuedOperations, fakeQueue.PushArgsForCall(0))
				enqueuedOperations = append(enqueuedOperations, fakeQueue.PushArgsForCall(1))

				Ω(enqueuedOperations).Should(ConsistOf(operation1, operation2))
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
			fakeTimeProvider.Increment(pollInterval + 1)
		})

		itPerformsBatchOperations()

		Context("and elapses again", func() {
			BeforeEach(func() {
				fakeTimeProvider.Increment(pollInterval)
			})

			itPerformsBatchOperations()
		})
	})

	Context("when the poll interval has not elapsed", func() {
		JustBeforeEach(func() {
			fakeTimeProvider.Increment(pollInterval - 1)
		})

		It("does not fetch batch operations", func() {
			Consistently(fakeGenerator.BatchOperationsCallCount).Should(BeZero())
		})
	})
})
