package main_test

import (
	"strconv"
	"testing"

	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var cellID string
var representativePath string
var etcdRunner *etcdstorerunner.ETCDClusterRunner
var etcdPort, natsPort int
var serverPort int
var consulRunner *consuladapter.ClusterRunner
var consulSession *consuladapter.Session

const assetsPath = "../../../../cloudfoundry/storeadapter/assets/"

func TestRep(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rep Integration Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	representative, err := gexec.Build("github.com/cloudfoundry-incubator/rep/cmd/rep", "-race")
	Expect(err).NotTo(HaveOccurred())
	return []byte(representative)
}, func(representative []byte) {
	representativePath = string(representative)

	cellID = "the-rep-id-" + strconv.Itoa(GinkgoParallelNode())

	etcdPort = 4001 + GinkgoParallelNode()
	serverPort = 1800 + GinkgoParallelNode()

	etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1,
		&etcdstorerunner.SSLConfig{
			CertFile: assetsPath + "server.crt",
			KeyFile:  assetsPath + "server.key",
			CAFile:   assetsPath + "ca.crt",
		})

	consulRunner = consuladapter.NewClusterRunner(
		9001+config.GinkgoConfig.ParallelNode*consuladapter.PortOffsetLength,
		1,
		"http",
	)

	etcdRunner.Start()
	consulRunner.Start()
})

var _ = BeforeEach(func() {
	consulRunner.WaitUntilReady()
	consulRunner.Reset()

	etcdRunner.Reset()

	consulSession = consulRunner.NewSession("a-session")
})

var _ = SynchronizedAfterSuite(func() {
	if etcdRunner != nil {
		etcdRunner.KillWithFire()
	}
	if consulRunner != nil {
		consulRunner.Stop()
	}
	if runner != nil {
		runner.KillWithFire()
	}
}, func() {
	gexec.CleanupBuildArtifacts()
})
