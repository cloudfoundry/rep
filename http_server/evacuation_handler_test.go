package http_server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"

	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context/fake_evacuation_context"
	"github.com/cloudfoundry-incubator/rep/http_server"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EvacuationHandler", func() {
	Describe("ServeHTTP", func() {
		var (
			logger          *lagertest.TestLogger
			fakeEvacuatable *fake_evacuation_context.FakeEvacuatable
			handler         *http_server.EvacuationHandler

			responseRecorder *httptest.ResponseRecorder
			request          *http.Request
		)

		BeforeEach(func() {
			logger = lagertest.NewTestLogger("test")
			fakeEvacuatable = new(fake_evacuation_context.FakeEvacuatable)
			handler = http_server.NewEvacuationHandler(logger, fakeEvacuatable)
		})

		Context("when receiving a request", func() {
			BeforeEach(func() {
				responseRecorder = httptest.NewRecorder()

				var err error
				request, err = http.NewRequest("POST", "/evacuate", nil)
				Ω(err).ShouldNot(HaveOccurred())

				handler.ServeHTTP(responseRecorder, request)
			})

			It("starts evacuation", func() {
				Ω(fakeEvacuatable.EvacuateCallCount()).Should(Equal(1))
			})

			It("responds with 202 ACCEPTED", func() {
				Ω(responseRecorder.Code).Should(Equal(http.StatusAccepted))
			})

			It("returns the location of the Ping endpoint", func() {
				var responseValues map[string]string
				err := json.Unmarshal(responseRecorder.Body.Bytes(), &responseValues)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(responseValues).Should(HaveKey("ping_path"))
				Ω(responseValues["ping_path"]).Should(Equal("/ping"))
			})
		})
	})
})
