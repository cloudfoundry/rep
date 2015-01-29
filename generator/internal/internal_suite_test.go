package internal_test

import (
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

func TestInternal(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Internal Suite")
}

var _ = BeforeSuite(func() {
	etcdRunner = etcdstorerunner.NewETCDClusterRunner(5001+config.GinkgoConfig.ParallelNode, 1)
	etcdClient = etcdRunner.Adapter()
})

var _ = AfterSuite(func() {
	etcdClient.Disconnect()
	etcdRunner.Stop()
})
