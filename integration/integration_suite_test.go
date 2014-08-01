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

var _ = BeforeSuite(func() {
	var err error

	executorID = "the-rep-id-" + strconv.Itoa(GinkgoParallelNode())

	etcdPort = 4001 + GinkgoParallelNode()
	natsPort = 4222 + GinkgoParallelNode()
	schedulerPort = 56000 + GinkgoParallelNode()

	natsRunner = natsrunner.NewNATSRunner(natsPort)
	etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1)

	representativePath, err = gexec.Build("github.com/cloudfoundry-incubator/rep", "-race")
	Ω(err).ShouldNot(HaveOccurred())
})

var _ = BeforeEach(func() {
	etcdRunner.Start()
	natsRunner.Start()
})

var _ = AfterEach(func(done Done) {
	natsRunner.Stop()
	etcdRunner.Stop()
	close(done)
})

var _ = AfterSuite(func(done Done) {
	gexec.CleanupBuildArtifacts()
	if etcdRunner != nil {
		etcdRunner.Stop()
	}
	if runner != nil {
		runner.KillWithFire()
	}
	close(done)
})
