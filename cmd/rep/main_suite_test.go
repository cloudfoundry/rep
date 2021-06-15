package main_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"code.cloudfoundry.org/bbs"
	bbsconfig "code.cloudfoundry.org/bbs/cmd/bbs/config"
	bbstestrunner "code.cloudfoundry.org/bbs/cmd/bbs/testrunner"
	"code.cloudfoundry.org/bbs/encryption"
	"code.cloudfoundry.org/bbs/test_helpers"
	"code.cloudfoundry.org/bbs/test_helpers/sqlrunner"
	"code.cloudfoundry.org/consuladapter/consulrunner"
	"code.cloudfoundry.org/diego-logging-client/testhelpers"
	"code.cloudfoundry.org/durationjson"
	"code.cloudfoundry.org/go-loggregator/v8/rpc/loggregator_v2"
	"code.cloudfoundry.org/inigo/helpers/portauthority"
	"code.cloudfoundry.org/lager/lagerflags"
	"code.cloudfoundry.org/locket"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/onsi/gomega/ghttp"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"
	"google.golang.org/grpc/grpclog"
)

var (
	cellID              string
	representativePath  string
	serverPort          uint16
	serverPortSecurable uint16
	consulRunner        *consulrunner.ClusterRunner

	bbsConfig        bbsconfig.BBSConfig
	bbsBinPath       string
	bbsURL           *url.URL
	bbsRunner        *ginkgomon.Runner
	bbsProcess       ifrit.Process
	bbsClient        bbs.InternalClient
	auctioneerServer *ghttp.Server
	locketBinPath    string
	node             int

	testIngressServer *testhelpers.TestIngressServer

	testMetricsChan   chan *loggregator_v2.Envelope
	signalMetricsChan chan struct{}

	sqlProcess    ifrit.Process
	sqlRunner     sqlrunner.SQLRunner
	portAllocator portauthority.PortAllocator

	fixturesPath = path.Join(os.Getenv("DIEGO_RELEASE_DIR"), "src/code.cloudfoundry.org/rep/cmd/rep/fixtures")
)

func TestRep(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rep Integration Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	bbsConfig, err := gexec.Build("code.cloudfoundry.org/bbs/cmd/bbs", "-race")
	Expect(err).NotTo(HaveOccurred())

	locketPath, err := gexec.Build("code.cloudfoundry.org/locket/cmd/locket", "-race")
	Expect(err).NotTo(HaveOccurred())

	representative, err := gexec.Build("code.cloudfoundry.org/rep/cmd/rep", "-race")
	Expect(err).NotTo(HaveOccurred())

	return []byte(strings.Join([]string{representative, locketPath, bbsConfig}, ","))
}, func(pathsByte []byte) {

	node = GinkgoParallelNode()
	startPort := 1050 * node
	portRange := 1000
	endPort := startPort + portRange

	var err error
	portAllocator, err = portauthority.New(startPort, endPort)
	Expect(err).NotTo(HaveOccurred())

	grpclog.SetLogger(log.New(ioutil.Discard, "", 0))

	// tests here are fairly Eventually driven which tends to flake out under
	// load (for insignificant reasons); bump the default a bit higher than the
	// default (1 second)
	SetDefaultEventuallyTimeout(5 * time.Second)

	strPath := string(pathsByte)
	representativePath = strings.Split(strPath, ",")[0]
	locketBinPath = strings.Split(strPath, ",")[1]
	bbsBinPath = strings.Split(strPath, ",")[2]

	cellID = "the_rep_id-" + strconv.Itoa(GinkgoParallelNode())

	serverPort, err = portAllocator.ClaimPorts(1)
	Expect(err).NotTo(HaveOccurred())
	serverPortSecurable, err = portAllocator.ClaimPorts(1)
	Expect(err).NotTo(HaveOccurred())

	dbName := fmt.Sprintf("diego_%d", GinkgoParallelNode())

	sqlRunner = test_helpers.NewSQLRunner(dbName)
	sqlProcess = ginkgomon.Invoke(sqlRunner)

	consulRunner = consulrunner.NewClusterRunner(
		consulrunner.ClusterRunnerConfig{
			StartingPort: 9001 + config.GinkgoConfig.ParallelNode*consulrunner.PortOffsetLength,
			NumNodes:     1,
			Scheme:       "http",
		},
	)

	consulRunner.Start()

	bbsPort, err := portAllocator.ClaimPorts(2)
	Expect(err).NotTo(HaveOccurred())
	healthPort := bbsPort + 1
	bbsAddress := fmt.Sprintf("127.0.0.1:%d", bbsPort)
	healthAddress := fmt.Sprintf("127.0.0.1:%d", healthPort)

	bbsURL = &url.URL{
		Scheme: "https",
		Host:   bbsAddress,
	}

	fixturesPath := path.Join(os.Getenv("DIEGO_RELEASE_DIR"), "src/code.cloudfoundry.org/rep/cmd/rep/fixtures")

	auctioneerServer = ghttp.NewServer()
	auctioneerServer.UnhandledRequestStatusCode = http.StatusAccepted
	auctioneerServer.AllowUnhandledRequests = true

	bbsConfig = bbsconfig.BBSConfig{
		ListenAddress:                  bbsAddress,
		AdvertiseURL:                   bbsURL.String(),
		AuctioneerAddress:              auctioneerServer.URL(),
		DatabaseDriver:                 sqlRunner.DriverName(),
		DatabaseConnectionString:       sqlRunner.ConnectionString(),
		DetectConsulCellRegistrations:  true,
		ConsulCluster:                  consulRunner.ConsulCluster(),
		HealthAddress:                  healthAddress,
		LocksLocketEnabled:             false,
		CellRegistrationsLocketEnabled: false,
		EncryptionConfig: encryption.EncryptionConfig{
			EncryptionKeys: map[string]string{"label": "key"},
			ActiveKeyLabel: "label",
		},

		CaFile:   path.Join(fixturesPath, "green-certs", "server-ca.crt"),
		CertFile: path.Join(fixturesPath, "green-certs", "server.crt"),
		KeyFile:  path.Join(fixturesPath, "green-certs", "server.key"),

		SessionName:                     "bbs",
		CommunicationTimeout:            durationjson.Duration(10 * time.Second),
		RequireSSL:                      true,
		DesiredLRPCreationTimeout:       durationjson.Duration(1 * time.Minute),
		ExpireCompletedTaskDuration:     durationjson.Duration(2 * time.Minute),
		ExpirePendingTaskDuration:       durationjson.Duration(30 * time.Minute),
		EnableConsulServiceRegistration: false,
		ConvergeRepeatInterval:          durationjson.Duration(30 * time.Second),
		KickTaskDuration:                durationjson.Duration(30 * time.Second),
		LockTTL:                         durationjson.Duration(locket.DefaultSessionTTL),
		LockRetryInterval:               durationjson.Duration(locket.RetryInterval),
		ReportInterval:                  durationjson.Duration(1 * time.Minute),
		ConvergenceWorkers:              20,
		UpdateWorkers:                   1000,
		TaskCallbackWorkers:             1000,
		MaxOpenDatabaseConnections:      200,
		MaxIdleDatabaseConnections:      200,
		AuctioneerRequireTLS:            false,
		RepClientSessionCacheSize:       0,
		RepRequireTLS:                   false,
		LagerConfig: lagerflags.LagerConfig{
			LogLevel: "info",
		},
	}
})

var _ = BeforeEach(func() {
	consulRunner.WaitUntilReady()
	consulRunner.Reset()

	bbsRunner = bbstestrunner.New(bbsBinPath, bbsConfig)
	bbsProcess = ginkgomon.Invoke(bbsRunner)

	metronCAFile := path.Join(fixturesPath, "metron", "CA.crt")
	metronServerCertFile := path.Join(fixturesPath, "metron", "metron.crt")
	metronServerKeyFile := path.Join(fixturesPath, "metron", "metron.key")

	var err error
	testIngressServer, err = testhelpers.NewTestIngressServer(metronServerCertFile, metronServerKeyFile, metronCAFile)
	Expect(err).NotTo(HaveOccurred())
	Expect(testIngressServer.Start()).To(Succeed())

	receiversChan := testIngressServer.Receivers()

	testMetricsChan, signalMetricsChan = testhelpers.TestMetricChan(receiversChan)
})

var _ = AfterEach(func() {
	sqlRunner.Reset()

	ginkgomon.Kill(bbsProcess)
})

var _ = SynchronizedAfterSuite(func() {
	ginkgomon.Kill(sqlProcess, "10s")
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
