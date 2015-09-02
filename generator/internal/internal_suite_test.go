package internal_test

import (
	"fmt"

	"github.com/cloudfoundry-incubator/bbs"
	bbsrunner "github.com/cloudfoundry-incubator/bbs/cmd/bbs/testrunner"
	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/cloudfoundry-incubator/consuladapter/consulrunner"
	"github.com/cloudfoundry/storeadapter"
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
var etcdClient storeadapter.StoreAdapter
var consulRunner *consulrunner.ClusterRunner
var consulSession *consuladapter.Session
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
	etcdClient = etcdRunner.Adapter(nil)

	consulRunner = consulrunner.NewClusterRunner(
		9001+config.GinkgoConfig.ParallelNode*consulrunner.PortOffsetLength,
		1,
		"http",
	)

	etcdRunner.Start()
	bbsBinPath := string(payload)
	bbsAddress := fmt.Sprintf("127.0.0.1:%d", 13000+GinkgoParallelNode())
	bbsProcess = ginkgomon.Invoke(bbsrunner.New(bbsBinPath, bbsrunner.Args{
		Address:           bbsAddress,
		AdvertiseURL:      "http://" + bbsAddress,
		AuctioneerAddress: "some-address",
		EtcdCluster:       etcdRunner.NodeURLS()[0],
		ConsulCluster:     consulRunner.ConsulCluster(),
	}))
	bbsClient = bbs.NewClient("http://" + bbsAddress)

	consulRunner.Start()
	consulRunner.WaitUntilReady()
})

var _ = SynchronizedAfterSuite(func() {
	ginkgomon.Kill(bbsProcess)
	consulRunner.Stop()
	etcdClient.Disconnect()
	etcdRunner.KillWithFire()
}, func() {
	gexec.CleanupBuildArtifacts()
})

var _ = BeforeEach(func() {
	consulRunner.Reset()
	consulSession = consulRunner.NewSession("a-session")
})
