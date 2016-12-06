package internal_test

import (
	"fmt"

	"code.cloudfoundry.org/bbs"
	bbsrunner "code.cloudfoundry.org/bbs/cmd/bbs/testrunner"
	"code.cloudfoundry.org/bbs/test_helpers"
	"code.cloudfoundry.org/bbs/test_helpers/sqlrunner"
	"code.cloudfoundry.org/consuladapter/consulrunner"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"

	"testing"
)

var (
	sqlRunner    sqlrunner.SQLRunner
	sqlProcess   ifrit.Process
	consulRunner *consulrunner.ClusterRunner
	bbsArgs      bbsrunner.Args
	bbsBinPath   string
	bbsRunner    *ginkgomon.Runner
	bbsProcess   ifrit.Process
	bbsClient    bbs.InternalClient
)

func TestInternal(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Internal Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	bbsBinPath, err := gexec.Build("code.cloudfoundry.org/bbs/cmd/bbs")
	Expect(err).NotTo(HaveOccurred())
	return []byte(bbsBinPath)
}, func(payload []byte) {
	dbName := fmt.Sprintf("diego_%d", GinkgoParallelNode())
	sqlRunner = test_helpers.NewSQLRunner(dbName)
	sqlProcess = ginkgomon.Invoke(sqlRunner)

	consulRunner = consulrunner.NewClusterRunner(
		9001+config.GinkgoConfig.ParallelNode*consulrunner.PortOffsetLength,
		1,
		"http",
	)

	bbsPort := 13000 + GinkgoParallelNode()*2
	healthPort := bbsPort + 1
	bbsAddress := fmt.Sprintf("127.0.0.1:%d", bbsPort)
	healthAddress := fmt.Sprintf("127.0.0.1:%d", healthPort)

	bbsBinPath = string(payload)
	bbsArgs = bbsrunner.Args{
		Address:                  bbsAddress,
		AdvertiseURL:             "http://" + bbsAddress,
		AuctioneerAddress:        "some-address",
		DatabaseDriver:           sqlRunner.DriverName(),
		DatabaseConnectionString: sqlRunner.ConnectionString(),
		ConsulCluster:            consulRunner.ConsulCluster(),
		HealthAddress:            healthAddress,

		EncryptionKeys: []string{"label:key"},
		ActiveKeyLabel: "label",
	}
	bbsClient = bbs.NewClient("http://" + bbsAddress)

	consulRunner.Start()
	consulRunner.WaitUntilReady()
})

var _ = SynchronizedAfterSuite(func() {
	consulRunner.Stop()
	ginkgomon.Kill(sqlProcess)
}, func() {
	gexec.CleanupBuildArtifacts()
})

var _ = BeforeEach(func() {
	consulRunner.Reset()
	bbsRunner = bbsrunner.New(bbsBinPath, bbsArgs)
	bbsProcess = ginkgomon.Invoke(bbsRunner)
})

var _ = AfterEach(func() {
	ginkgomon.Kill(bbsProcess)
})
