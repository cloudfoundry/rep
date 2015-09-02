package main_test

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cloudfoundry-incubator/bbs"
	bbstestrunner "github.com/cloudfoundry-incubator/bbs/cmd/bbs/testrunner"
	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/cloudfoundry-incubator/consuladapter/consulrunner"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/onsi/gomega/ghttp"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"
)

var cellID string
var representativePath string
var etcdRunner *etcdstorerunner.ETCDClusterRunner
var etcdPort, natsPort int
var serverPort int
var consulRunner *consulrunner.ClusterRunner
var consulSession *consuladapter.Session

var bbsArgs bbstestrunner.Args
var bbsBinPath string
var bbsURL *url.URL
var bbsRunner *ginkgomon.Runner
var bbsProcess ifrit.Process
var bbsClient bbs.Client
var auctioneerServer *ghttp.Server

func TestRep(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rep Integration Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	bbsConfig, err := gexec.Build("github.com/cloudfoundry-incubator/bbs/cmd/bbs", "-race")
	Expect(err).NotTo(HaveOccurred())

	representative, err := gexec.Build("github.com/cloudfoundry-incubator/rep/cmd/rep", "-race")
	Expect(err).NotTo(HaveOccurred())

	return []byte(strings.Join([]string{representative, bbsConfig}, ","))
}, func(pathsByte []byte) {
	// tests here are fairly Eventually driven which tends to flake out under
	// load (for insignificant reasons); bump the default a bit higher than the
	// default (1 second)
	SetDefaultEventuallyTimeout(5 * time.Second)

	path := string(pathsByte)
	representativePath = strings.Split(path, ",")[0]
	bbsBinPath = strings.Split(path, ",")[1]

	cellID = "the-rep-id-" + strconv.Itoa(GinkgoParallelNode())

	etcdPort = 4001 + GinkgoParallelNode()
	serverPort = 1800 + GinkgoParallelNode()

	etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1, nil)

	consulRunner = consulrunner.NewClusterRunner(
		9001+config.GinkgoConfig.ParallelNode*consulrunner.PortOffsetLength,
		1,
		"http",
	)

	etcdRunner.Start()
	consulRunner.Start()

	bbsAddress := fmt.Sprintf("127.0.0.1:%d", 13000+GinkgoParallelNode())

	bbsURL = &url.URL{
		Scheme: "http",
		Host:   bbsAddress,
	}

	bbsClient = bbs.NewClient(bbsURL.String())

	auctioneerServer = ghttp.NewServer()
	auctioneerServer.UnhandledRequestStatusCode = http.StatusAccepted
	auctioneerServer.AllowUnhandledRequests = true

	etcdUrl := fmt.Sprintf("http://127.0.0.1:%d", etcdPort)
	bbsArgs = bbstestrunner.Args{
		Address:           bbsAddress,
		AdvertiseURL:      bbsURL.String(),
		AuctioneerAddress: auctioneerServer.URL(),
		EtcdCluster:       etcdUrl,
		ConsulCluster:     consulRunner.ConsulCluster(),
	}
})

var _ = BeforeEach(func() {
	consulRunner.WaitUntilReady()
	consulRunner.Reset()

	etcdRunner.Reset()

	bbsRunner = bbstestrunner.New(bbsBinPath, bbsArgs)
	bbsProcess = ginkgomon.Invoke(bbsRunner)

	consulSession = consulRunner.NewSession("a-session")
})

var _ = AfterEach(func() {
	if bbsProcess != nil {
		ginkgomon.Kill(bbsProcess)
	}
})

var _ = SynchronizedAfterSuite(func() {
	if etcdRunner != nil {
		etcdRunner.KillWithFire()
	}
	if consulRunner != nil {
		consulRunner.Stop()
	}
	if runner != nil {
		runner.KillWithFire()
	}
	if auctioneerServer != nil {
		auctioneerServer.Close()
	}
}, func() {
	gexec.CleanupBuildArtifacts()
})
