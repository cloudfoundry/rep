package handlers_test

import (
	"net/http"

	"code.cloudfoundry.org/rep"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PingHandler", func() {
	It("responds with 200 OK", func() {
		status, _ := Request(rep.PingRoute, nil, nil)
		Expect(status).To(Equal(http.StatusOK))
	})

	It("emits the request metrics", func() {
		Request(rep.PingRoute, nil, nil)

		Expect(fakeRequestMetrics.IncrementRequestsStartedCounterCallCount()).To(Equal(1))
		calledRequestType, delta := fakeRequestMetrics.IncrementRequestsStartedCounterArgsForCall(0)
		Expect(delta).To(Equal(1))
		Expect(calledRequestType).To(Equal("Ping"))

		Expect(fakeRequestMetrics.IncrementRequestsInFlightCounterCallCount()).To(Equal(1))
		calledRequestType, delta = fakeRequestMetrics.IncrementRequestsInFlightCounterArgsForCall(0)
		Expect(delta).To(Equal(1))
		Expect(calledRequestType).To(Equal("Ping"))

		Expect(fakeRequestMetrics.DecrementRequestsInFlightCounterCallCount()).To(Equal(1))
		calledRequestType, delta = fakeRequestMetrics.DecrementRequestsInFlightCounterArgsForCall(0)
		Expect(delta).To(Equal(1))
		Expect(calledRequestType).To(Equal("Ping"))

		Expect(fakeRequestMetrics.UpdateLatencyCallCount()).To(Equal(1))
		calledRequestType, _ = fakeRequestMetrics.UpdateLatencyArgsForCall(0)
		Expect(calledRequestType).To(Equal("Ping"))

		Expect(fakeRequestMetrics.IncrementRequestsSucceededCounterCallCount()).To(Equal(1))
		calledRequestType, delta = fakeRequestMetrics.IncrementRequestsSucceededCounterArgsForCall(0)
		Expect(delta).To(Equal(1))
		Expect(calledRequestType).To(Equal("Ping"))

		Expect(fakeRequestMetrics.IncrementRequestsFailedCounterCallCount()).To(Equal(0))
	})
})
