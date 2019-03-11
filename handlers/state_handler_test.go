package handlers_test

import (
	"errors"
	"net/http"
	"time"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/rep"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("State", func() {
	var (
		repState       rep.CellState
		requestLatency time.Duration
	)

	BeforeEach(func() {
		repState = rep.CellState{
			RootFSProviders: rep.RootFSProviders{"docker": rep.ArbitraryRootFSProvider{}},
		}
		requestLatency = 50 * time.Millisecond
		fakeLocalRep.StateStub = func(logger lager.Logger) (rep.CellState, bool, error) {
			time.Sleep(requestLatency)
			return repState, true, nil
		}
	})

	It("it returns whatever the state call returns", func() {
		status, body := Request(rep.StateRoute, nil, nil)
		Expect(status).To(Equal(http.StatusOK))
		Expect(body).To(MatchJSON(JSONFor(repState)))
		Expect(fakeLocalRep.StateCallCount()).To(Equal(1))
	})

	It("emits the request metrics", func() {
		Request(rep.StateRoute, nil, nil)

		Expect(fakeRequestMetrics.IncrementRequestsStartedCounterCallCount()).To(Equal(1))
		calledRequestType, delta := fakeRequestMetrics.IncrementRequestsStartedCounterArgsForCall(0)
		Expect(delta).To(Equal(1))
		Expect(calledRequestType).To(Equal("State"))

		Expect(fakeRequestMetrics.IncrementRequestsInFlightCounterCallCount()).To(Equal(1))
		calledRequestType, delta = fakeRequestMetrics.IncrementRequestsInFlightCounterArgsForCall(0)
		Expect(delta).To(Equal(1))
		Expect(calledRequestType).To(Equal("State"))

		Expect(fakeRequestMetrics.DecrementRequestsInFlightCounterCallCount()).To(Equal(1))
		calledRequestType, delta = fakeRequestMetrics.DecrementRequestsInFlightCounterArgsForCall(0)
		Expect(delta).To(Equal(1))
		Expect(calledRequestType).To(Equal("State"))

		Expect(fakeRequestMetrics.UpdateLatencyCallCount()).To(Equal(1))
		calledRequestType, calledLatency := fakeRequestMetrics.UpdateLatencyArgsForCall(0)
		Expect(calledRequestType).To(Equal("State"))
		Expect(calledLatency).To(BeNumerically("~", requestLatency, 5*time.Millisecond))

		Expect(fakeRequestMetrics.IncrementRequestsSucceededCounterCallCount()).To(Equal(1))
		calledRequestType, delta = fakeRequestMetrics.IncrementRequestsSucceededCounterArgsForCall(0)
		Expect(delta).To(Equal(1))
		Expect(calledRequestType).To(Equal("State"))

		Expect(fakeRequestMetrics.IncrementRequestsFailedCounterCallCount()).To(Equal(0))
	})

	Context("when the state call is not healthy", func() {
		BeforeEach(func() {
			fakeLocalRep.StateReturns(repState, false, nil)
		})

		It("returns a StatusServiceUnavailable", func() {
			status, body := Request(rep.StateRoute, nil, nil)
			Expect(status).To(Equal(http.StatusServiceUnavailable))
			Expect(body).To(MatchJSON(JSONFor(repState)))
			Expect(fakeLocalRep.StateCallCount()).To(Equal(1))
		})
	})

	Context("when the state call fails", func() {
		BeforeEach(func() {
			fakeLocalRep.StateReturns(rep.CellState{}, false, errors.New("boom"))
		})

		It("fails", func() {
			status, body := Request(rep.StateRoute, nil, nil)
			Expect(status).To(Equal(http.StatusInternalServerError))
			Expect(body).To(BeEmpty())
			Expect(fakeLocalRep.StateCallCount()).To(Equal(1))
		})

		It("emits the failed request metrics", func() {
			Request(rep.StateRoute, nil, nil)

			Expect(fakeRequestMetrics.IncrementRequestsSucceededCounterCallCount()).To(Equal(0))

			Expect(fakeRequestMetrics.IncrementRequestsFailedCounterCallCount()).To(Equal(1))
			calledRequestType, delta := fakeRequestMetrics.IncrementRequestsFailedCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("State"))
		})
	})
})
