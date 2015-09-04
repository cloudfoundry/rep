package rep_test

import (
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/cloudfoundry-incubator/cf_http"
	executorfakes "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context/fake_evacuation_context"
	"github.com/cloudfoundry-incubator/rep/handlers"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper/fake_lrp_stopper"
	"github.com/cloudfoundry-incubator/rep/repfakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/ghttp"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/rata"

	"testing"
)

var (
	cfHttpTimeout    time.Duration
	auctionRep       *repfakes.FakeClient
	server           *httptest.Server
	serverThatErrors *ghttp.Server

	client, clientForServerThatErrors rep.Client

	fakeLRPStopper     *fake_lrp_stopper.FakeLRPStopper
	fakeExecutorClient *executorfakes.FakeClient
	fakeEvacuatable    *fake_evacuation_context.FakeEvacuatable
)

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

	auctionRep = &repfakes.FakeClient{}
	fakeLRPStopper = &fake_lrp_stopper.FakeLRPStopper{}
	fakeExecutorClient = &executorfakes.FakeClient{}
	fakeEvacuatable = &fake_evacuation_context.FakeEvacuatable{}

	handler, err := rata.NewRouter(rep.Routes, handlers.New(auctionRep, fakeLRPStopper, fakeExecutorClient, fakeEvacuatable, logger))
	Expect(err).NotTo(HaveOccurred())
	server = httptest.NewServer(handler)

	client = rep.NewClient(&http.Client{}, server.URL)

	serverThatErrors = ghttp.NewServer()
	erroringHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		serverThatErrors.CloseClientConnections()
	})
	//5 erroringHandlers should be more than enough: none of the individual tests should make more than 5 requests to this server
	serverThatErrors.AppendHandlers(erroringHandler, erroringHandler, erroringHandler, erroringHandler, erroringHandler)

	clientForServerThatErrors = rep.NewClient(&http.Client{}, serverThatErrors.URL())
})

var _ = AfterEach(func() {
	server.Close()
	serverThatErrors.Close()
})
