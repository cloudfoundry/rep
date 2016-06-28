package handlers_test

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	executorfakes "code.cloudfoundry.org/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context/fake_evacuation_context"
	"github.com/cloudfoundry-incubator/rep/handlers"
	"github.com/cloudfoundry-incubator/rep/repfakes"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/tedsuo/rata"

	"testing"
)

func TestAuctionHttpHandlers(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "AuctionHttpHandlers Suite")
}

var server *httptest.Server
var requestGenerator *rata.RequestGenerator
var client *http.Client
var fakeLocalRep *repfakes.FakeSimClient
var repGuid string

var _ = BeforeEach(func() {
	logger := lagertest.NewTestLogger("handlers")

	fakeLocalRep = new(repfakes.FakeSimClient)
	fakeExecutorClient := new(executorfakes.FakeClient)
	fakeEvacuatable := new(fake_evacuation_context.FakeEvacuatable)
	handler, err := rata.NewRouter(rep.Routes, handlers.New(fakeLocalRep, fakeExecutorClient, fakeEvacuatable, logger))
	Expect(err).NotTo(HaveOccurred())
	server = httptest.NewServer(handler)

	requestGenerator = rata.NewRequestGenerator(server.URL, rep.Routes)

	client = &http.Client{}
})

var _ = AfterEach(func() {
	server.Close()
})

func JSONFor(obj interface{}) string {
	marshalled, err := json.Marshal(obj)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	return string(marshalled)
}

func JSONReaderFor(obj interface{}) io.Reader {
	marshalled, err := json.Marshal(obj)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	return bytes.NewBuffer(marshalled)
}

func Request(name string, params rata.Params, body io.Reader) (statusCode int, responseBody []byte) {
	request, err := requestGenerator.CreateRequest(name, params, body)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	response, err := client.Do(request)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	responseBody, err = ioutil.ReadAll(response.Body)
	response.Body.Close()

	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	return response.StatusCode, responseBody
}
