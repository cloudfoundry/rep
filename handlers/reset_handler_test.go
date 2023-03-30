package handlers_test

import (
	"errors"
	"net/http"
	"time"

	"code.cloudfoundry.org/rep"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Reset", func() {
	Context("when the reset succeeds", func() {
		var requestLatency time.Duration

		BeforeEach(func() {
			requestLatency = 50 * time.Millisecond
			fakeLocalRep.ResetStub = func() error {
				time.Sleep(requestLatency)
				return nil
			}
		})

		It("succeeds", func() {
			status, body := Request(rep.SimResetRoute, nil, nil)
			Expect(status).To(Equal(http.StatusOK))
			Expect(body).To(BeEmpty())

			Expect(fakeLocalRep.ResetCallCount()).To(Equal(1))
		})

		It("emits the request metrics", func() {
			Request(rep.SimResetRoute, nil, nil)

			Expect(fakeRequestMetrics.IncrementRequestsStartedCounterCallCount()).To(Equal(1))
			calledRequestType, delta := fakeRequestMetrics.IncrementRequestsStartedCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("Reset"))

			Expect(fakeRequestMetrics.IncrementRequestsInFlightCounterCallCount()).To(Equal(1))
			calledRequestType, delta = fakeRequestMetrics.IncrementRequestsInFlightCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("Reset"))

			Expect(fakeRequestMetrics.DecrementRequestsInFlightCounterCallCount()).To(Equal(1))
			calledRequestType, delta = fakeRequestMetrics.DecrementRequestsInFlightCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("Reset"))

			Expect(fakeRequestMetrics.UpdateLatencyCallCount()).To(Equal(1))
			calledRequestType, calledLatency := fakeRequestMetrics.UpdateLatencyArgsForCall(0)
			Expect(calledRequestType).To(Equal("Reset"))
			Expect(calledLatency).To(BeNumerically("~", requestLatency, 25*time.Millisecond))

			Expect(fakeRequestMetrics.IncrementRequestsSucceededCounterCallCount()).To(Equal(1))
			calledRequestType, delta = fakeRequestMetrics.IncrementRequestsSucceededCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("Reset"))

			Expect(fakeRequestMetrics.IncrementRequestsFailedCounterCallCount()).To(Equal(0))
		})
	})

	Context("when the reset fails", func() {
		BeforeEach(func() {
			fakeLocalRep.ResetReturns(errors.New("boom"))
		})

		It("fails", func() {
			status, body := Request(rep.SimResetRoute, nil, nil)
			Expect(status).To(Equal(http.StatusInternalServerError))
			Expect(body).To(BeEmpty())

			Expect(fakeLocalRep.ResetCallCount()).To(Equal(1))
		})

		It("emits the failed request metrics", func() {
			Request(rep.SimResetRoute, nil, nil)

			Expect(fakeRequestMetrics.IncrementRequestsSucceededCounterCallCount()).To(Equal(0))

			Expect(fakeRequestMetrics.IncrementRequestsFailedCounterCallCount()).To(Equal(1))
			calledRequestType, delta := fakeRequestMetrics.IncrementRequestsFailedCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("Reset"))
		})
	})
})
