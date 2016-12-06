package main_test

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"code.cloudfoundry.org/bbs"
	bbstestrunner "code.cloudfoundry.org/bbs/cmd/bbs/testrunner"
	"code.cloudfoundry.org/bbs/test_helpers"
	"code.cloudfoundry.org/bbs/test_helpers/sqlrunner"
	"code.cloudfoundry.org/consuladapter/consulrunner"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/onsi/gomega/ghttp"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"
)

var (
	cellID              string
	representativePath  string
	natsPort            int
	serverPort          int
	serverPortSecurable int
	consulRunner        *consulrunner.ClusterRunner

	bbsArgs          bbstestrunner.Args
	bbsBinPath       string
	bbsURL           *url.URL
	bbsRunner        *ginkgomon.Runner
	bbsProcess       ifrit.Process
	bbsClient        bbs.InternalClient
	auctioneerServer *ghttp.Server

	sqlProcess ifrit.Process
	sqlRunner  sqlrunner.SQLRunner
)

func TestRep(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rep Integration Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	bbsConfig, err := gexec.Build("code.cloudfoundry.org/bbs/cmd/bbs", "-race")
	Expect(err).NotTo(HaveOccurred())

	representative, err := gexec.Build("code.cloudfoundry.org/rep/cmd/rep", "-race")
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

	cellID = "the_rep_id-" + strconv.Itoa(GinkgoParallelNode())

	serverPort = 1800 + GinkgoParallelNode()
	serverPortSecurable = 1901 + GinkgoParallelNode()

	dbName := fmt.Sprintf("diego_%d", GinkgoParallelNode())

	sqlRunner = test_helpers.NewSQLRunner(dbName)
	sqlProcess = ginkgomon.Invoke(sqlRunner)

	consulRunner = consulrunner.NewClusterRunner(
		9001+config.GinkgoConfig.ParallelNode*consulrunner.PortOffsetLength,
		1,
		"http",
	)

	consulRunner.Start()

	bbsPort := 13000 + GinkgoParallelNode()*2
	healthPort := bbsPort + 1
	bbsAddress := fmt.Sprintf("127.0.0.1:%d", bbsPort)
	healthAddress := fmt.Sprintf("127.0.0.1:%d", healthPort)

	bbsURL = &url.URL{
		Scheme: "http",
		Host:   bbsAddress,
	}

	bbsClient = bbs.NewClient(bbsURL.String())

	auctioneerServer = ghttp.NewServer()
	auctioneerServer.UnhandledRequestStatusCode = http.StatusAccepted
	auctioneerServer.AllowUnhandledRequests = true

	bbsArgs = bbstestrunner.Args{
		Address:                  bbsAddress,
		AdvertiseURL:             bbsURL.String(),
		AuctioneerAddress:        auctioneerServer.URL(),
		DatabaseDriver:           sqlRunner.DriverName(),
		DatabaseConnectionString: sqlRunner.ConnectionString(),
		ConsulCluster:            consulRunner.ConsulCluster(),
		HealthAddress:            healthAddress,

		EncryptionKeys: []string{"label:key"},
		ActiveKeyLabel: "label",
	}
})

var _ = BeforeEach(func() {
	consulRunner.WaitUntilReady()
	consulRunner.Reset()

	bbsRunner = bbstestrunner.New(bbsBinPath, bbsArgs)
	bbsProcess = ginkgomon.Invoke(bbsRunner)
})

var _ = AfterEach(func() {
	sqlRunner.Reset()

	ginkgomon.Kill(bbsProcess)
})

var _ = SynchronizedAfterSuite(func() {
	ginkgomon.Kill(sqlProcess)
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
