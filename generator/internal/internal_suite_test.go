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
var consulPort int
var consulRunner consuladapter.ClusterRunner
var consulAdapter consuladapter.Adapter

func TestInternal(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Internal Suite")
}

var _ = BeforeSuite(func() {
	etcdRunner = etcdstorerunner.NewETCDClusterRunner(5001+config.GinkgoConfig.ParallelNode, 1)
	etcdClient = etcdRunner.Adapter()

	consulPort = 9001 + config.GinkgoConfig.ParallelNode*consuladapter.PortOffsetLength
	consulRunner = consuladapter.NewClusterRunner(
		consulPort,
		1,
		"http",
	)
	consulAdapter = consulRunner.NewAdapter()

	etcdRunner.Start()
	consulRunner.Start()
})

var _ = BeforeEach(func() {
	consulRunner.Reset()
})

var _ = AfterSuite(func() {
	consulRunner.Stop()
	etcdClient.Disconnect()
	etcdRunner.KillWithFire()
})
