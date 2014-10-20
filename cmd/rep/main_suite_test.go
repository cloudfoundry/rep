package main_test

import (
	"strconv"
	"testing"

	"github.com/cloudfoundry/gunk/diegonats"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"
)

var executorID string
var representativePath string
var etcdRunner *etcdstorerunner.ETCDClusterRunner
var gnatsdProcess ifrit.Process
var natsClient diegonats.NATSClient
var etcdPort, natsPort, schedulerPort int

func TestRep(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rep Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	representative, err := gexec.Build("github.com/cloudfoundry-incubator/rep/cmd/rep", "-race")
	Ω(err).ShouldNot(HaveOccurred())
	return []byte(representative)
}, func(representative []byte) {
	representativePath = string(representative)

	executorID = "the-rep-id-" + strconv.Itoa(GinkgoParallelNode())

	etcdPort = 4001 + GinkgoParallelNode()
	natsPort = 4222 + GinkgoParallelNode()
	schedulerPort = 56000 + GinkgoParallelNode()

	etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1)
})

var _ = BeforeEach(func() {
	etcdRunner.Start()
	gnatsdProcess, natsClient = diegonats.StartGnatsd(natsPort)
})

var _ = AfterEach(func() {
	ginkgomon.Interrupt(gnatsdProcess)
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
