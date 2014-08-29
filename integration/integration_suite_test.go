package integration_test

import (
	"strconv"
	"testing"

	"github.com/cloudfoundry/gunk/natsrunner"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var executorID string
var representativePath string
var etcdRunner *etcdstorerunner.ETCDClusterRunner
var natsRunner *natsrunner.NATSRunner
var etcdPort, natsPort, schedulerPort int

func TestRepresentativeMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	representative, err := gexec.Build("github.com/cloudfoundry-incubator/rep", "-race")
	Ω(err).ShouldNot(HaveOccurred())
	return []byte(representative)
}, func(representative []byte) {
	representativePath = string(representative)

	executorID = "the-rep-id-" + strconv.Itoa(GinkgoParallelNode())

	etcdPort = 4001 + GinkgoParallelNode()
	natsPort = 4222 + GinkgoParallelNode()
	schedulerPort = 56000 + GinkgoParallelNode()

	natsRunner = natsrunner.NewNATSRunner(natsPort)
	etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1)
})

var _ = BeforeEach(func() {
	etcdRunner.Start()
	natsRunner.Start()
})

var _ = AfterEach(func() {
	natsRunner.Stop()
	etcdRunner.Stop()
})

var _ = SynchronizedAfterSuite(func() {
	if etcdRunner != nil {
		etcdRunner.Stop()
	}
	if natsRunner != nil {
		natsRunner.Stop()
	}
	if runner != nil {
		runner.KillWithFire()
	}
}, func() {
	gexec.CleanupBuildArtifacts()
})
