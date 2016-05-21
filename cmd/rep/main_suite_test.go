package main_test

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cloudfoundry-incubator/bbs"
	bbstestrunner "github.com/cloudfoundry-incubator/bbs/cmd/bbs/testrunner"
	"github.com/cloudfoundry-incubator/consuladapter/consulrunner"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	"github.com/cloudfoundry/storeadapter/storerunner/mysqlrunner"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/onsi/gomega/ghttp"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"
)

var (
	cellID             string
	representativePath string
	etcdRunner         *etcdstorerunner.ETCDClusterRunner
	etcdPort, natsPort int
	serverPort         int
	consulRunner       *consulrunner.ClusterRunner

	bbsArgs          bbstestrunner.Args
	bbsBinPath       string
	bbsURL           *url.URL
	bbsRunner        *ginkgomon.Runner
	bbsProcess       ifrit.Process
	bbsClient        bbs.InternalClient
	auctioneerServer *ghttp.Server

	mySQLProcess ifrit.Process
	mySQLRunner  *mysqlrunner.MySQLRunner
	useSQL       bool
)

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
	useSQL = os.Getenv("USE_SQL") != ""

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

	if useSQL {
		mySQLRunner = mysqlrunner.NewMySQLRunner(fmt.Sprintf("diego_%d", GinkgoParallelNode()))
		mySQLProcess = ginkgomon.Invoke(mySQLRunner)
	}

	consulRunner = consulrunner.NewClusterRunner(
		9001+config.GinkgoConfig.ParallelNode*consulrunner.PortOffsetLength,
		1,
		"http",
	)

	etcdRunner.Start()
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

	etcdUrl := fmt.Sprintf("http://127.0.0.1:%d", etcdPort)
	bbsArgs = bbstestrunner.Args{
		Address:           bbsAddress,
		AdvertiseURL:      bbsURL.String(),
		AuctioneerAddress: auctioneerServer.URL(),
		EtcdCluster:       etcdUrl,
		ConsulCluster:     consulRunner.ConsulCluster(),
		HealthAddress:     healthAddress,

		EncryptionKeys: []string{"label:key"},
		ActiveKeyLabel: "label",
	}

	if useSQL {
		bbsArgs.DatabaseDriver = "mysql"
		bbsArgs.DatabaseConnectionString = mySQLRunner.ConnectionString()
	}
})

var _ = BeforeEach(func() {
	consulRunner.WaitUntilReady()
	consulRunner.Reset()

	etcdRunner.Reset()

	bbsRunner = bbstestrunner.New(bbsBinPath, bbsArgs)
	bbsProcess = ginkgomon.Invoke(bbsRunner)
})

var _ = AfterEach(func() {
	if useSQL {
		mySQLRunner.Reset()
	}

	if bbsProcess != nil {
		ginkgomon.Kill(bbsProcess)
	}
})

var _ = SynchronizedAfterSuite(func() {
	ginkgomon.Kill(mySQLProcess)
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
