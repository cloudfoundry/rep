package http_server_test

import (
	"net/http"
	"net/http/httptest"

	"github.com/cloudfoundry-incubator/rep/http_server"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PingHandler", func() {
	var (
		pingHandler *http_server.PingHandler
		resp        *httptest.ResponseRecorder
		req         *http.Request
	)

	JustBeforeEach(func() {
		pingHandler = http_server.NewPingHandler()

		resp = httptest.NewRecorder()

		var err error
		req, err = http.NewRequest("GET", "/ping", nil)
		Ω(err).ShouldNot(HaveOccurred())

		pingHandler.ServeHTTP(resp, req)
	})

	It("responds with 200 OK", func() {
		Ω(resp.Code).Should(Equal(http.StatusOK))
	})
})
