package main_test

import (
	"strconv"
	"testing"

	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var cellID string
var representativePath string
var etcdRunner *etcdstorerunner.ETCDClusterRunner
var etcdPort, natsPort int
var auctionServerPort int

func TestRep(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rep Integration Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	representative, err := gexec.Build("github.com/cloudfoundry-incubator/rep/cmd/rep", "-race")
	Ω(err).ShouldNot(HaveOccurred())
	return []byte(representative)
}, func(representative []byte) {
	representativePath = string(representative)

	cellID = "the-rep-id-" + strconv.Itoa(GinkgoParallelNode())

	etcdPort = 4001 + GinkgoParallelNode()
	auctionServerPort = 1800 + GinkgoParallelNode()

	etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1)
})

var _ = BeforeEach(func() {
	etcdRunner.Start()
})

var _ = AfterEach(func() {
	etcdRunner.Stop()
})

var _ = SynchronizedAfterSuite(func() {
	if etcdRunner != nil {
		etcdRunner.Stop()
	}
	if runner != nil {
		runner.KillWithFire()
	}
}, func() {
	gexec.CleanupBuildArtifacts()
})
