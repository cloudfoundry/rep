package integration_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/auction/communication/nats/repnatsclient"
	"github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/rep/reprunner"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gunk/natsrunner"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/onsi/gomega/ghttp"

	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var representativePath string
var etcdRunner *etcdstorerunner.ETCDClusterRunner
var natsRunner *natsrunner.NATSRunner
var runner *reprunner.Runner

var _ = Describe("Main", func() {
	var (
		fakeExecutor *ghttp.Server
		bbs          *Bbs.BBS
	)

	BeforeEach(func() {
		natsRunner = natsrunner.NewNATSRunner(4001)
		fakeExecutor = ghttp.NewServer()

		// these tests only look for the start of a sequence of requests
		fakeExecutor.AllowUnhandledRequests = true

		etcdPort := 5001 + GinkgoParallelNode()
		schedulerPort := 56000 + GinkgoParallelNode()

		etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1)
		etcdRunner.Start()

		natsRunner.Start()

		bbs = Bbs.NewBBS(etcdRunner.Adapter(), timeprovider.NewTimeProvider())

		runner = reprunner.New(
			representativePath,
			"the-stack",
			"the-lrp-host",
			fmt.Sprintf("127.0.0.1:%d", schedulerPort),
			fakeExecutor.URL(),
			fmt.Sprintf("http://127.0.0.1:%d", etcdPort),
			"127.0.0.1:4001",
			"info",
			time.Second,
		)

		runner.Start()
	})

	AfterEach(func() {
		runner.KillWithFire()
		fakeExecutor.Close()
		etcdRunner.Stop()
		natsRunner.Stop()
	})

	Describe("when a kill signal is send to the representative", func() {
		BeforeEach(func() {
			runner.Stop()
		})

		It("should die", func() {
			Eventually(runner.Session.ExitCode).Should(Equal(0))
		})
	})

	Describe("maintaining presence", func() {
		var repPresence models.RepPresence

		BeforeEach(func() {
			Eventually(bbs.GetAllReps).Should(HaveLen(1))
			reps, err := bbs.GetAllReps()
			Ω(err).ShouldNot(HaveOccurred())
			repPresence = reps[0]
		})

		It("should maintain presence", func() {
			Ω(repPresence.Stack).Should(Equal("the-stack"))
			Ω(repPresence.RepID).ShouldNot(BeZero())
		})

		Context("when the presence fails to be maintained", func() {
			It("should not exit, but keep trying to maintain presence at the same ID", func() {
				etcdRunner.Stop()
				etcdRunner.Start()

				Eventually(bbs.GetAllReps).Should(HaveLen(1))
				reps, err := bbs.GetAllReps()
				Ω(err).ShouldNot(HaveOccurred())
				Ω(reps[0]).Should(Equal(repPresence))

				Ω(runner.Session).ShouldNot(gexec.Exit())
			})
		})
	})

	Describe("when a task is written to the BBS", func() {
		BeforeEach(func() {
			fakeExecutor.AppendHandlers(ghttp.CombineHandlers(
				ghttp.VerifyRequest("POST", "/containers/the-task-guid"),
				ghttp.RespondWith(http.StatusCreated, `{"executor_guid":"executor-guid","guid":"guid-123"}`)),
			)

			bbs.DesireTask(models.Task{
				Guid:  "the-task-guid",
				Stack: "the-stack",
			})
		})

		It("makes a request to the executor", func() {
			Eventually(fakeExecutor.ReceivedRequests).Should(HaveLen(1))
		})
	})

	Describe("acting as an auction representative", func() {
		BeforeEach(func() {
			fakeExecutor.AppendHandlers(ghttp.CombineHandlers(
				ghttp.VerifyRequest("GET", "/resources/total"),
				ghttp.RespondWithJSONEncoded(http.StatusOK, api.ExecutorResources{
					MemoryMB:   1024,
					DiskMB:     2048,
					Containers: 4,
				})),
			)
		})

		It("makes a request to the executor", func() {
			Eventually(bbs.GetAllReps).Should(HaveLen(1))
			reps, err := bbs.GetAllReps()
			Ω(err).ShouldNot(HaveOccurred())
			repID := reps[0].RepID

			client := repnatsclient.New(natsRunner.MessageBus, time.Second, 10*time.Second)
			resources := client.TotalResources(repID)
			Ω(resources).Should(Equal(auctiontypes.Resources{
				MemoryMB:   1024,
				DiskMB:     2048,
				Containers: 4,
			}))
		})
	})
})

func TestRepresentativeMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}

var _ = BeforeSuite(func() {
	var err error
	representativePath, err = gexec.Build("github.com/cloudfoundry-incubator/rep", "-race")
	Ω(err).ShouldNot(HaveOccurred())
})

var _ = AfterSuite(func() {
	gexec.CleanupBuildArtifacts()
	if etcdRunner != nil {
		etcdRunner.Stop()
	}
	if runner != nil {
		runner.KillWithFire()
	}
})
