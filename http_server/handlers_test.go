package http_server_test

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"github.com/cloudfoundry-incubator/rep/http_server"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper/fake_lrp_stopper"
	"github.com/cloudfoundry-incubator/runtime-schema/models"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Handlers", func() {
	Describe("StopLRPInstanceHandler", func() {
		var stopInstanceHandler *http_server.StopLRPInstanceHandler
		var fakeStopper *fake_lrp_stopper.FakeLRPStopper
		var resp *httptest.ResponseRecorder
		var req *http.Request

		BeforeEach(func() {
			var err error
			fakeStopper = &fake_lrp_stopper.FakeLRPStopper{}
			stopInstanceHandler = http_server.NewStopLRPInstanceHandler(fakeStopper)

			resp = httptest.NewRecorder()

			req, err = http.NewRequest("POST", "", nil)
			Ω(err).ShouldNot(HaveOccurred())
		})

		JustBeforeEach(func() {
			stopInstanceHandler.ServeHTTP(resp, req)
		})

		Context("when the request is valid", func() {
			var actualLRP models.ActualLRP

			BeforeEach(func() {
				var err error
				actualLRP, err = models.NewActualLRP(
					"p-guid",
					"i-guid",
					"cell-id",
					"webbernet",
					1,
				)
				Ω(err).ShouldNot(HaveOccurred())

				payload, err := json.Marshal(actualLRP)
				Ω(err).ShouldNot(HaveOccurred())

				req.Body = ioutil.NopCloser(bytes.NewBuffer(payload))
			})

			It("responds with 202 Accepted", func() {
				Ω(resp.Code).Should(Equal(http.StatusAccepted))
			})

			It("stops the instance", func() {
				Ω(fakeStopper.StopInstanceCallCount()).Should(Equal(1))

				Ω(fakeStopper.StopInstanceArgsForCall(0)).Should(Equal(actualLRP))
			})
		})

		Context("when the request is invalid", func() {
			BeforeEach(func() {
				req.Body = ioutil.NopCloser(bytes.NewBufferString("foo"))
			})

			It("responds with 400 Bad Request", func() {
				Ω(resp.Code).Should(Equal(http.StatusBadRequest))
			})

			It("does not attempt to stop the instance", func() {
				Ω(fakeStopper.StopInstanceCallCount()).Should(Equal(0))
			})
		})
	})
})
