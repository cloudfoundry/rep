package handlers_test

import (
	"errors"
	"net/http"

	"code.cloudfoundry.org/executor"
	"code.cloudfoundry.org/rep"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("CancelTask", func() {
	var params map[string]string

	BeforeEach(func() {
		params = map[string]string{"task_guid": "some-guid"}
	})

	Context("when the container deletion succeeds", func() {
		BeforeEach(func() {
			fakeExecutorClient.DeleteContainerReturns(nil)
		})

		It("responds with Accepted status", func() {
			status, _ := Request(rep.CancelTaskRoute, params, nil)
			Expect(status).To(Equal(http.StatusAccepted))

			Eventually(fakeExecutorClient.DeleteContainerCallCount).Should(Equal(1))
			_, taskGuidArg := fakeExecutorClient.DeleteContainerArgsForCall(0)
			Expect(taskGuidArg).To(Equal("some-guid"))
		})

		It("emits request metrics", func() {
			Request(rep.CancelTaskRoute, params, nil)

			Expect(fakeRequestMetrics.IncrementRequestsStartedCounterCallCount()).To(Equal(1))
			calledRequestType, delta := fakeRequestMetrics.IncrementRequestsStartedCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("CancelTask"))

			Expect(fakeRequestMetrics.IncrementRequestsInFlightCounterCallCount()).To(Equal(1))
			calledRequestType, delta = fakeRequestMetrics.IncrementRequestsInFlightCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("CancelTask"))

			Expect(fakeRequestMetrics.DecrementRequestsInFlightCounterCallCount()).To(Equal(1))
			calledRequestType, delta = fakeRequestMetrics.DecrementRequestsInFlightCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("CancelTask"))

			Expect(fakeRequestMetrics.UpdateLatencyCallCount()).To(Equal(1))
			calledRequestType, _ = fakeRequestMetrics.UpdateLatencyArgsForCall(0)
			Expect(calledRequestType).To(Equal("CancelTask"))

			Consistently(fakeRequestMetrics.IncrementRequestsFailedCounterCallCount()).Should(Equal(0))

			Eventually(fakeRequestMetrics.IncrementRequestsSucceededCounterCallCount()).Should(Equal(1))
			calledRequestType, delta = fakeRequestMetrics.IncrementRequestsSucceededCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("CancelTask"))
		})
	})

	Context("when the container does not exist", func() {
		BeforeEach(func() {
			fakeExecutorClient.DeleteContainerReturns(executor.ErrContainerNotFound)
		})

		It("responds with Accepted status", func() {
			status, _ := Request(rep.CancelTaskRoute, params, nil)
			Expect(status).To(Equal(http.StatusAccepted))

			Eventually(fakeExecutorClient.DeleteContainerCallCount).Should(Equal(1))
			_, taskGuidArg := fakeExecutorClient.DeleteContainerArgsForCall(0)
			Expect(taskGuidArg).To(Equal("some-guid"))
		})

		It("emits success request metric", func() {
			Request(rep.CancelTaskRoute, params, nil)

			Consistently(fakeRequestMetrics.IncrementRequestsFailedCounterCallCount()).Should(Equal(0))

			Eventually(fakeRequestMetrics.IncrementRequestsSucceededCounterCallCount()).Should(Equal(1))
			calledRequestType, delta := fakeRequestMetrics.IncrementRequestsSucceededCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("CancelTask"))
		})
	})

	Context("when the container deletion fails", func() {
		BeforeEach(func() {
			fakeExecutorClient.DeleteContainerReturns(errors.New("uh-oh"))
		})

		It("responds with Accepted status", func() {
			status, _ := Request(rep.CancelTaskRoute, params, nil)
			Expect(status).To(Equal(http.StatusAccepted))

			Eventually(fakeExecutorClient.DeleteContainerCallCount).Should(Equal(1))
			_, taskGuidArg := fakeExecutorClient.DeleteContainerArgsForCall(0)
			Expect(taskGuidArg).To(Equal("some-guid"))
		})

		It("emits failed request metric", func() {
			Request(rep.CancelTaskRoute, params, nil)

			Consistently(fakeRequestMetrics.IncrementRequestsSucceededCounterCallCount()).Should(Equal(0))

			Eventually(fakeRequestMetrics.IncrementRequestsFailedCounterCallCount()).Should(Equal(1))
			calledRequestType, delta := fakeRequestMetrics.IncrementRequestsFailedCounterArgsForCall(0)
			Expect(delta).To(Equal(1))
			Expect(calledRequestType).To(Equal("CancelTask"))
		})
	})
})
