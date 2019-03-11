package handlers_test

import (
	"bytes"
	"errors"
	"net/http"
	"time"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/rep"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Perform", func() {
	Context("with valid JSON", func() {
		var (
			requestedWork, failedWork rep.Work
			requestLatency            time.Duration
		)

		BeforeEach(func() {
			resourceA := rep.NewResource(128, 256, 256)
			placementContraintA := rep.NewPlacementConstraint("some-rootfs", nil, nil)
			resourceB := rep.NewResource(256, 512, 256)
			placementContraintB := rep.NewPlacementConstraint("some-rootfs", nil, nil)
			resourceC := rep.NewResource(512, 1024, 256)
			placementContraintC := rep.NewPlacementConstraint("some-rootfs", nil, nil)

			requestedWork = rep.Work{
				Tasks: []rep.Task{
					rep.NewTask("a", "domain", resourceA, placementContraintA),
					rep.NewTask("b", "domain", resourceB, placementContraintB),
				},
			}

			failedWork = rep.Work{
				Tasks: []rep.Task{
					rep.NewTask("c", "domain", resourceC, placementContraintC),
				},
			}

			requestLatency = 50 * time.Millisecond
		})

		Context("and no perform error", func() {
			BeforeEach(func() {
				fakeLocalRep.PerformStub = func(logger lager.Logger, work rep.Work) (rep.Work, error) {
					time.Sleep(requestLatency)
					return failedWork, nil
				}
			})

			It("succeeds, returning any failed work", func() {
				status, body := Request(rep.PerformRoute, nil, JSONReaderFor(requestedWork))
				Expect(status).To(Equal(http.StatusOK))
				Expect(body).To(MatchJSON(JSONFor(failedWork)))

				Expect(fakeLocalRep.PerformCallCount()).To(Equal(1))
				_, actualWork := fakeLocalRep.PerformArgsForCall(0)
				Expect(actualWork).To(Equal(requestedWork))
			})

			It("emits the request metrics", func() {
				Request(rep.PerformRoute, nil, JSONReaderFor(requestedWork))

				Expect(fakeRequestMetrics.IncrementRequestsStartedCounterCallCount()).To(Equal(1))
				calledRequestType, delta := fakeRequestMetrics.IncrementRequestsStartedCounterArgsForCall(0)
				Expect(delta).To(Equal(1))
				Expect(calledRequestType).To(Equal("Perform"))

				Expect(fakeRequestMetrics.IncrementRequestsInFlightCounterCallCount()).To(Equal(1))
				calledRequestType, delta = fakeRequestMetrics.IncrementRequestsInFlightCounterArgsForCall(0)
				Expect(delta).To(Equal(1))
				Expect(calledRequestType).To(Equal("Perform"))

				Expect(fakeRequestMetrics.DecrementRequestsInFlightCounterCallCount()).To(Equal(1))
				calledRequestType, delta = fakeRequestMetrics.DecrementRequestsInFlightCounterArgsForCall(0)
				Expect(delta).To(Equal(1))
				Expect(calledRequestType).To(Equal("Perform"))

				Expect(fakeRequestMetrics.UpdateLatencyCallCount()).To(Equal(1))
				calledRequestType, calledLatency := fakeRequestMetrics.UpdateLatencyArgsForCall(0)
				Expect(calledRequestType).To(Equal("Perform"))
				Expect(calledLatency).To(BeNumerically("~", requestLatency, 5*time.Millisecond))

				Expect(fakeRequestMetrics.IncrementRequestsSucceededCounterCallCount()).To(Equal(1))
				calledRequestType, delta = fakeRequestMetrics.IncrementRequestsSucceededCounterArgsForCall(0)
				Expect(delta).To(Equal(1))
				Expect(calledRequestType).To(Equal("Perform"))

				Expect(fakeRequestMetrics.IncrementRequestsFailedCounterCallCount()).To(Equal(0))
			})
		})

		Context("and a perform error", func() {
			BeforeEach(func() {
				fakeLocalRep.PerformReturns(failedWork, errors.New("kaboom"))
			})

			It("fails, returning nothing", func() {
				status, body := Request(rep.PerformRoute, nil, JSONReaderFor(requestedWork))
				Expect(status).To(Equal(http.StatusInternalServerError))
				Expect(body).To(BeEmpty())

				Expect(fakeLocalRep.PerformCallCount()).To(Equal(1))
				_, actualWork := fakeLocalRep.PerformArgsForCall(0)
				Expect(actualWork).To(Equal(requestedWork))
			})

			It("emits the failed request metrics", func() {
				Request(rep.PerformRoute, nil, JSONReaderFor(requestedWork))

				Expect(fakeRequestMetrics.IncrementRequestsSucceededCounterCallCount()).To(Equal(0))

				Expect(fakeRequestMetrics.IncrementRequestsFailedCounterCallCount()).To(Equal(1))
				calledRequestType, delta := fakeRequestMetrics.IncrementRequestsFailedCounterArgsForCall(0)
				Expect(delta).To(Equal(1))
				Expect(calledRequestType).To(Equal("Perform"))
			})
		})
	})

	Context("with invalid JSON", func() {
		It("fails", func() {
			status, body := Request(rep.PerformRoute, nil, bytes.NewBufferString("∆"))
			Expect(status).To(Equal(http.StatusBadRequest))
			Expect(body).To(BeEmpty())

			Expect(fakeLocalRep.PerformCallCount()).To(Equal(0))
		})

		It("emits the failed request metric", func() {
			Request(rep.PerformRoute, nil, bytes.NewBufferString("∆"))

			Expect(fakeRequestMetrics.IncrementRequestsSucceededCounterCallCount()).To(Equal(0))

			Expect(fakeRequestMetrics.IncrementRequestsFailedCounterCallCount()).To(Equal(1))
			calledRequestType, delta := fakeRequestMetrics.IncrementRequestsFailedCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("Perform"))
		})
	})
})
