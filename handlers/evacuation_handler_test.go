package handlers_test

import (
	"encoding/json"
	"net/http"

	"code.cloudfoundry.org/rep"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EvacuationHandler", func() {
	Context("when receiving a request", func() {
		It("starts evacuation", func() {
			Request(rep.EvacuateRoute, nil, nil)
			Expect(fakeEvacuatable.EvacuateCallCount()).To(Equal(1))
		})

		It("responds with 202 ACCEPTED", func() {
			status, _ := Request(rep.EvacuateRoute, nil, nil)
			Expect(status).To(Equal(http.StatusAccepted))
		})

		It("returns the location of the Ping endpoint", func() {
			_, body := Request(rep.EvacuateRoute, nil, nil)

			var responseValues map[string]string
			err := json.Unmarshal(body, &responseValues)
			Expect(err).NotTo(HaveOccurred())
			Expect(responseValues).To(HaveKey("ping_path"))
			Expect(responseValues["ping_path"]).To(Equal("/ping"))
		})

		It("emits the request metrics", func() {
			Request(rep.EvacuateRoute, nil, nil)

			Expect(fakeRequestMetrics.IncrementRequestsStartedCounterCallCount()).To(Equal(1))
			calledRequestType, delta := fakeRequestMetrics.IncrementRequestsStartedCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("Evacuation"))

			Expect(fakeRequestMetrics.IncrementRequestsInFlightCounterCallCount()).To(Equal(1))
			calledRequestType, delta = fakeRequestMetrics.IncrementRequestsInFlightCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("Evacuation"))

			Expect(fakeRequestMetrics.DecrementRequestsInFlightCounterCallCount()).To(Equal(1))
			calledRequestType, delta = fakeRequestMetrics.DecrementRequestsInFlightCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("Evacuation"))

			Expect(fakeRequestMetrics.UpdateLatencyCallCount()).To(Equal(1))
			calledRequestType, _ = fakeRequestMetrics.UpdateLatencyArgsForCall(0)
			Expect(calledRequestType).To(Equal("Evacuation"))

			Expect(fakeRequestMetrics.IncrementRequestsSucceededCounterCallCount()).To(Equal(1))
			calledRequestType, delta = fakeRequestMetrics.IncrementRequestsSucceededCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("Evacuation"))

			Expect(fakeRequestMetrics.IncrementRequestsFailedCounterCallCount()).To(Equal(0))
		})
	})
})
