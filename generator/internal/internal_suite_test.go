package internal_test

import (
	"fmt"

	"github.com/cloudfoundry-incubator/bbs"
	bbsrunner "github.com/cloudfoundry-incubator/bbs/cmd/bbs/testrunner"
	"github.com/cloudfoundry-incubator/consuladapter/consulrunner"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"

	"testing"
)

var etcdRunner *etcdstorerunner.ETCDClusterRunner
var consulRunner *consulrunner.ClusterRunner
var bbsArgs bbsrunner.Args
var bbsBinPath string
var bbsRunner *ginkgomon.Runner
var bbsProcess ifrit.Process
var bbsClient bbs.Client

func TestInternal(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Internal Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	bbsBinPath, err := gexec.Build("github.com/cloudfoundry-incubator/bbs/cmd/bbs")
	Expect(err).NotTo(HaveOccurred())
	return []byte(bbsBinPath)
}, func(payload []byte) {
	etcdRunner = etcdstorerunner.NewETCDClusterRunner(5001+config.GinkgoConfig.ParallelNode, 1, nil)

	consulRunner = consulrunner.NewClusterRunner(
		9001+config.GinkgoConfig.ParallelNode*consulrunner.PortOffsetLength,
		1,
		"http",
	)

	etcdRunner.Start()
	bbsAddress := fmt.Sprintf("127.0.0.1:%d", 13000+GinkgoParallelNode())
	bbsBinPath = string(payload)
	bbsArgs = bbsrunner.Args{
		Address:           bbsAddress,
		AdvertiseURL:      "http://" + bbsAddress,
		AuctioneerAddress: "some-address",
		EtcdCluster:       etcdRunner.NodeURLS()[0],
		ConsulCluster:     consulRunner.ConsulCluster(),
	}
	bbsClient = bbs.NewClient("http://" + bbsAddress)

	consulRunner.Start()
	consulRunner.WaitUntilReady()
})

var _ = SynchronizedAfterSuite(func() {
	consulRunner.Stop()
	etcdRunner.KillWithFire()
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
