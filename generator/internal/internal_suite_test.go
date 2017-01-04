package internal_test

import (
	"fmt"
	"time"

	"code.cloudfoundry.org/bbs"
	bbsconfig "code.cloudfoundry.org/bbs/cmd/bbs/config"
	bbsrunner "code.cloudfoundry.org/bbs/cmd/bbs/testrunner"
	"code.cloudfoundry.org/bbs/encryption"
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
	bbsConfig    bbsconfig.BBSConfig
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
		consulrunner.ClusterRunnerConfig{
			StartingPort: 9001 + config.GinkgoConfig.ParallelNode*consulrunner.PortOffsetLength,
			NumNodes:     1,
			Scheme:       "http",
		},
	)

	bbsPort := 13000 + GinkgoParallelNode()*2
	healthPort := bbsPort + 1
	bbsAddress := fmt.Sprintf("127.0.0.1:%d", bbsPort)
	healthAddress := fmt.Sprintf("127.0.0.1:%d", healthPort)

	bbsBinPath = string(payload)
	bbsConfig = bbsconfig.BBSConfig{
		ListenAddress:            bbsAddress,
		AdvertiseURL:             "http://" + bbsAddress,
		AuctioneerAddress:        "some-address",
		DatabaseDriver:           sqlRunner.DriverName(),
		DatabaseConnectionString: sqlRunner.ConnectionString(),
		ConsulCluster:            consulRunner.ConsulCluster(),
		HealthAddress:            healthAddress,
		EncryptionConfig: encryption.EncryptionConfig{
			EncryptionKeys: map[string]string{"label": "key"},
			ActiveKeyLabel: "label",
		},
	}
	bbsClient = bbs.NewClient("http://" + bbsAddress)

	consulRunner.Start()
	consulRunner.WaitUntilReady()
})

var _ = SynchronizedAfterSuite(func() {
	consulRunner.Stop()
	ginkgomon.Kill(sqlProcess, 5*time.Second)
}, func() {
	gexec.CleanupBuildArtifacts()
})

var _ = BeforeEach(func() {
	consulRunner.Reset()
	bbsRunner = bbsrunner.New(bbsBinPath, bbsConfig)
	bbsProcess = ginkgomon.Invoke(bbsRunner)
})

var _ = AfterEach(func() {
	ginkgomon.Kill(bbsProcess)
})
