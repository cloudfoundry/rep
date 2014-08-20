package services_bbs_test

import (
	"os"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"

	. "github.com/cloudfoundry-incubator/runtime-schema/bbs/services_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/storeadapter"
)

var _ = Describe("Fetching all Executors", func() {
	var (
		bbs                    *ServicesBBS
		interval               = time.Second
		heartbeat1             ifrit.Process
		heartbeat2             ifrit.Process
		firstExecutorPresence  models.ExecutorPresence
		secondExecutorPresence models.ExecutorPresence
	)

	BeforeEach(func() {
		bbs = New(etcdClient, lagertest.NewTestLogger("test"))

		firstExecutorPresence = models.ExecutorPresence{
			ExecutorID: "first-rep",
			Stack:      "lucid64",
		}

		secondExecutorPresence = models.ExecutorPresence{
			ExecutorID: "second-rep",
			Stack:      ".Net",
		}

		interval = 1 * time.Second

		heartbeat1 = ifrit.Envoke(bbs.NewExecutorHeartbeat(firstExecutorPresence, interval))
		heartbeat2 = ifrit.Envoke(bbs.NewExecutorHeartbeat(secondExecutorPresence, interval))
	})

	AfterEach(func() {
		heartbeat1.Signal(os.Interrupt)
		heartbeat2.Signal(os.Interrupt)
		Eventually(heartbeat1.Wait()).Should(Receive(BeNil()))
		Eventually(heartbeat2.Wait()).Should(Receive(BeNil()))
	})

	Describe("MaintainExecutorPresence", func() {
		It("should put /executor/EXECUTOR_ID in the store with a TTL", func() {
			node, err := etcdClient.Get("/v1/executor/" + firstExecutorPresence.ExecutorID)
			Ω(err).ShouldNot(HaveOccurred())
			Ω(node.TTL).ShouldNot(BeZero())
			Ω(node.Value).Should(MatchJSON(firstExecutorPresence.ToJSON()))
		})
	})

	Describe("GetAllExecutors", func() {
		Context("when there are available Executors", func() {
			It("should get from /v1/executor/", func() {
				executorPresences, err := bbs.GetAllExecutors()
				Ω(err).ShouldNot(HaveOccurred())
				Ω(executorPresences).Should(HaveLen(2))
				Ω(executorPresences).Should(ContainElement(firstExecutorPresence))
				Ω(executorPresences).Should(ContainElement(secondExecutorPresence))
			})

			Context("when there is unparsable JSON in there...", func() {
				BeforeEach(func() {
					etcdClient.Create(storeadapter.StoreNode{
						Key:   shared.ExecutorSchemaPath("blah"),
						Value: []byte("ß"),
					})
				})

				It("should ignore the unparsable JSON and move on", func() {
					executorPresences, err := bbs.GetAllExecutors()
					Ω(err).ShouldNot(HaveOccurred())
					Ω(executorPresences).Should(HaveLen(2))
					Ω(executorPresences).Should(ContainElement(firstExecutorPresence))
					Ω(executorPresences).Should(ContainElement(secondExecutorPresence))
				})
			})
		})

		Context("when there are none", func() {
			BeforeEach(func() {
				heartbeat1.Signal(os.Interrupt)
				heartbeat2.Signal(os.Interrupt)
				Eventually(heartbeat1.Wait()).Should(Receive(BeNil()))
				Eventually(heartbeat2.Wait()).Should(Receive(BeNil()))
			})

			It("should return empty", func() {
				reps, err := bbs.GetAllExecutors()
				Ω(err).ShouldNot(HaveOccurred())
				Ω(reps).Should(BeEmpty())
			})
		})
	})
})
