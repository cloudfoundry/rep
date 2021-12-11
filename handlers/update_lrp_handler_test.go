package handlers_test

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	"code.cloudfoundry.org/bbs/models"
	"code.cloudfoundry.org/executor"
	executorfakes "code.cloudfoundry.org/executor/fakes"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	"code.cloudfoundry.org/rep"
	"code.cloudfoundry.org/rep/handlers"
	"code.cloudfoundry.org/routing-info/internalroutes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("UpdateLRPInstanceHandler", func() {
	var (
		updateInstanceHandler *handlers.UpdateLRPInstanceHandler
		fakeClient            *executorfakes.FakeClient
		resp                  *httptest.ResponseRecorder
		req                   *http.Request
		logger                *lagertest.TestLogger
		processGuid           string
		instanceGuid          string
		internalRoutes        internalroutes.InternalRoutes
	)

	BeforeEach(func() {
		var err error

		fakeClient = &executorfakes.FakeClient{}

		logger = lagertest.NewTestLogger("test")
		logger.RegisterSink(lager.NewWriterSink(GinkgoWriter, lager.DEBUG))

		updateInstanceHandler = handlers.NewUpdateLRPInstanceHandler(fakeClient, fakeRequestMetrics)

		resp = httptest.NewRecorder()

		processGuid = "process-guid"
		instanceGuid = "instance-guid"
		internalRoutes = internalroutes.InternalRoutes{
			{Hostname: "a.apps.internal"},
			{Hostname: "b.apps.internal"},
		}
		lrpUpdate := rep.NewLRPUpdate(instanceGuid, models.NewActualLRPKey(processGuid, 2, "test-domain"), internalRoutes)
		req, err = http.NewRequest("PUT", "", JSONReaderFor(lrpUpdate))
		Expect(err).NotTo(HaveOccurred())
	})

	JustBeforeEach(func() {
		updateInstanceHandler.ServeHTTP(resp, req, logger)
	})

	Context("when the request is valid", func() {
		BeforeEach(func() {
			values := make(url.Values)
			values.Set(":process_guid", processGuid)
			values.Set(":instance_guid", instanceGuid)
			req.URL.RawQuery = values.Encode()
		})

		Context("and UpdateContainer succeeds", func() {
			var requestLatency time.Duration

			BeforeEach(func() {
				requestLatency = 50 * time.Millisecond
				fakeClient.UpdateContainerStub = func(logger lager.Logger, updateReq *executor.UpdateRequest) error {
					time.Sleep(requestLatency)
					return nil
				}
			})

			It("responds with 202 Accepted", func() {
				Expect(resp.Code).To(Equal(http.StatusAccepted))
			})

			It("eventually updates the instance", func() {
				Eventually(fakeClient.UpdateContainerCallCount).Should(Equal(1))

				_, updateReq := fakeClient.UpdateContainerArgsForCall(0)
				Expect(updateReq.Guid).To(Equal(instanceGuid))
				Expect(updateReq.InternalRoutes).To(Equal(internalRoutes))
			})

			It("emits the request metrics", func() {
				Expect(fakeRequestMetrics.IncrementRequestsStartedCounterCallCount()).To(Equal(1))
				calledRequestType, delta := fakeRequestMetrics.IncrementRequestsStartedCounterArgsForCall(0)
				Expect(delta).To(Equal(1))
				Expect(calledRequestType).To(Equal("UpdateLRPInstance"))

				Expect(fakeRequestMetrics.IncrementRequestsInFlightCounterCallCount()).To(Equal(1))
				calledRequestType, delta = fakeRequestMetrics.IncrementRequestsInFlightCounterArgsForCall(0)
				Expect(delta).To(Equal(1))
				Expect(calledRequestType).To(Equal("UpdateLRPInstance"))

				Expect(fakeRequestMetrics.DecrementRequestsInFlightCounterCallCount()).To(Equal(1))
				calledRequestType, delta = fakeRequestMetrics.DecrementRequestsInFlightCounterArgsForCall(0)
				Expect(delta).To(Equal(1))
				Expect(calledRequestType).To(Equal("UpdateLRPInstance"))

				Expect(fakeRequestMetrics.UpdateLatencyCallCount()).To(Equal(1))
				calledRequestType, calledLatency := fakeRequestMetrics.UpdateLatencyArgsForCall(0)
				Expect(calledRequestType).To(Equal("UpdateLRPInstance"))
				Expect(calledLatency).To(BeNumerically("~", requestLatency, 25*time.Millisecond))

				Expect(fakeRequestMetrics.IncrementRequestsSucceededCounterCallCount()).To(Equal(1))
				calledRequestType, delta = fakeRequestMetrics.IncrementRequestsSucceededCounterArgsForCall(0)
				Expect(delta).To(Equal(1))
				Expect(calledRequestType).To(Equal("UpdateLRPInstance"))

				Expect(fakeRequestMetrics.IncrementRequestsFailedCounterCallCount()).To(Equal(0))
			})
		})

		Context("but UpdateContainer fails", func() {
			BeforeEach(func() {
				fakeClient.UpdateContainerReturns(errors.New("fail"))
			})

			It("responds with 500 Internal Server Error", func() {
				Expect(resp.Code).To(Equal(http.StatusInternalServerError))
			})

			It("emits the failed request metrics", func() {
				Expect(fakeRequestMetrics.IncrementRequestsSucceededCounterCallCount()).To(Equal(0))

				Expect(fakeRequestMetrics.IncrementRequestsFailedCounterCallCount()).To(Equal(1))
				calledRequestType, delta := fakeRequestMetrics.IncrementRequestsFailedCounterArgsForCall(0)
				Expect(delta).To(Equal(1))
				Expect(calledRequestType).To(Equal("UpdateLRPInstance"))
			})
		})
	})

	Context("when the request is invalid", func() {
		BeforeEach(func() {
			req.Body = ioutil.NopCloser(bytes.NewBufferString("foo"))
		})

		It("responds with 400 Bad Request", func() {
			Expect(resp.Code).To(Equal(http.StatusBadRequest))
		})

		It("does not attempt to update the instance", func() {
			Expect(fakeClient.UpdateContainerCallCount()).To(Equal(0))
		})

		It("emits the failed request metrics", func() {
			Expect(fakeRequestMetrics.IncrementRequestsSucceededCounterCallCount()).To(Equal(0))

			Expect(fakeRequestMetrics.IncrementRequestsFailedCounterCallCount()).To(Equal(1))
			calledRequestType, delta := fakeRequestMetrics.IncrementRequestsFailedCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("UpdateLRPInstance"))
		})
	})
})
