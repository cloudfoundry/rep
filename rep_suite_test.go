package rep_test

import (
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/auction/auctiontypes/fakes"
	"github.com/cloudfoundry-incubator/cf_http"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/handlers"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/ghttp"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/rata"

	"testing"
)

var cfHttpTimeout time.Duration
var auctionRep *fakes.FakeSimulationCellRep
var server *httptest.Server
var serverThatErrors *ghttp.Server
var client, clientForServerThatErrors auctiontypes.SimulationCellRep

func TestRep(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rep Suite")
}

var _ = BeforeSuite(func() {
	cfHttpTimeout = 1 * time.Second
	cf_http.Initialize(cfHttpTimeout)
})

var _ = BeforeEach(func() {
	logger := lagertest.NewTestLogger("test")

	auctionRep = &fakes.FakeSimulationCellRep{}

	handler, err := rata.NewRouter(rep.Routes, handlers.New(auctionRep, logger))
	Expect(err).NotTo(HaveOccurred())
	server = httptest.NewServer(handler)

	client = rep.NewClient(&http.Client{}, "rep-guid", server.URL, logger)

	serverThatErrors = ghttp.NewServer()
	erroringHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		serverThatErrors.CloseClientConnections()
	})
	//5 erroringHandlers should be more than enough: none of the individual tests should make more than 5 requests to this server
	serverThatErrors.AppendHandlers(erroringHandler, erroringHandler, erroringHandler, erroringHandler, erroringHandler)

	clientForServerThatErrors = rep.NewClient(&http.Client{}, "rep-guid", serverThatErrors.URL(), logger)
})

var _ = AfterEach(func() {
	server.Close()
	serverThatErrors.Close()
})
