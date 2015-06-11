package internal_test

import (
	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry/storeadapter"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"

	"testing"
)

var BBS *bbs.BBS
var etcdRunner *etcdstorerunner.ETCDClusterRunner
var etcdClient storeadapter.StoreAdapter
var consulRunner *consuladapter.ClusterRunner
var consulSession *consuladapter.Session

const assetsPath = "../../../../cloudfoundry/storeadapter/assets/"

func TestInternal(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Internal Suite")
}

var _ = BeforeSuite(func() {
	etcdRunner = etcdstorerunner.NewETCDClusterRunner(5001+config.GinkgoConfig.ParallelNode, 1,
		&etcdstorerunner.SSLConfig{
			CertFile: assetsPath + "server.crt",
			KeyFile:  assetsPath + "server.key",
			CAFile:   assetsPath + "ca.crt",
		})
	etcdClient = etcdRunner.Adapter(&etcdstorerunner.SSLConfig{
		CertFile: assetsPath + "client.crt",
		KeyFile:  assetsPath + "client.key",
		CAFile:   assetsPath + "ca.crt",
	})

	consulRunner = consuladapter.NewClusterRunner(
		9001+config.GinkgoConfig.ParallelNode*consuladapter.PortOffsetLength,
		1,
		"http",
	)

	etcdRunner.Start()
	consulRunner.Start()
	consulRunner.WaitUntilReady()
})

var _ = BeforeEach(func() {
	consulRunner.Reset()
	consulSession = consulRunner.NewSession("a-session")
})

var _ = AfterSuite(func() {
	consulRunner.Stop()
	etcdClient.Disconnect()
	etcdRunner.KillWithFire()
})
