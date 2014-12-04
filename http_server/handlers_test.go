package http_server_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"

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
			req, err = http.NewRequest("DELETE", "", nil)
			Ω(err).ShouldNot(HaveOccurred())
		})

		JustBeforeEach(func() {
			stopInstanceHandler.ServeHTTP(resp, req)
		})

		Context("when the request is valid", func() {
			BeforeEach(func() {
				req.Form = stopInstanceParams("p-guid", "1", "i-guid")
			})

			It("responds with 202 Accepted", func() {
				Ω(resp.Code).Should(Equal(http.StatusAccepted))
			})

			It("stops the instance", func() {
				Ω(fakeStopper.StopInstanceCallCount()).Should(Equal(1))
				Ω(fakeStopper.StopInstanceArgsForCall(0)).Should(Equal(models.StopLRPInstance{
					ProcessGuid:  "p-guid",
					InstanceGuid: "i-guid",
					Index:        1,
				}))
			})
		})

		Context("when the request is invalid", func() {
			var invalidParams = []paramTest{
				{"no process guid", stopInstanceParams("", "1", "i-guid")},
				{"index is NaaN", stopInstanceParams("p-guid", "foo", "i-guid")},
				{"no instance guid", stopInstanceParams("p-guid", "1", "")},
			}

			for _, paramTest := range invalidParams {
				var params = paramTest.params // bind params to the current iteration

				Context(paramTest.name, func() {
					BeforeEach(func() {
						req.Form = params
					})

					It("responds with 400 Bad Request", func() {
						Ω(resp.Code).Should(Equal(http.StatusBadRequest))
					})

					It("does not attempt to stop the instance", func() {
						Ω(fakeStopper.StopInstanceCallCount()).Should(Equal(0))
					})
				})
			}
		})
	})
})

type paramTest struct {
	name   string
	params url.Values
}

func stopInstanceParams(processGuid, index, instanceGuid string) url.Values {
	params := url.Values{}
	params.Set(":process_guid", processGuid)
	params.Set(":index", index)
	params.Set(":instance_guid", instanceGuid)
	return params
}
