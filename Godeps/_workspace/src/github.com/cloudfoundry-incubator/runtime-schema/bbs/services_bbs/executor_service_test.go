package services_bbs_test

import (
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/cloudfoundry-incubator/runtime-schema/bbs/services_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	steno "github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/storeadapter"
	"github.com/cloudfoundry/storeadapter/test_helpers"
)

var _ = Describe("Fetching all Executors", func() {
	var (
		bbs                    *ServicesBBS
		interval               time.Duration
		status                 <-chan bool
		err                    error
		firstPresence          Presence
		secondPresence         Presence
		firstExecutorPresence  models.ExecutorPresence
		secondExecutorPresence models.ExecutorPresence
	)

	BeforeEach(func() {
		logSink := steno.NewTestingSink()

		steno.Init(&steno.Config{
			Sinks: []steno.Sink{logSink},
		})

		logger := steno.NewLogger("the-logger")
		steno.EnterTestMode()

		bbs = New(etcdClient, logger)

		firstExecutorPresence = models.ExecutorPresence{
			ExecutorID: "first-rep",
			Stack:      "lucid64",
		}

		secondExecutorPresence = models.ExecutorPresence{
			ExecutorID: "second-rep",
			Stack:      ".Net",
		}
	})

	Describe("MaintainExecutorPresence", func() {
		var (
			executorPresence models.ExecutorPresence
			presence         Presence
		)

		BeforeEach(func() {
			executorPresence = models.ExecutorPresence{
				ExecutorID: "stubRep",
				Stack:      "pancakes",
			}
			interval = 1 * time.Second

			presence, status, err = bbs.MaintainExecutorPresence(interval, executorPresence)
			Ω(err).ShouldNot(HaveOccurred())

			reporter := test_helpers.NewStatusReporter(status)
			Eventually(reporter.Locked).Should(BeTrue())
		})

		AfterEach(func() {
			presence.Remove()
		})

		It("should put /executor/EXECUTOR_ID in the store with a TTL", func() {
			node, err := etcdClient.Get("/v1/executor/" + executorPresence.ExecutorID)
			Ω(err).ShouldNot(HaveOccurred())
			Ω(node.TTL).Should(Equal(uint64(interval.Seconds()))) // move to config one day

			jsonEncoded, _ := json.Marshal(executorPresence)
			Ω(node.Value).Should(MatchJSON(jsonEncoded))
		})
	})

	Describe("GetAllExecutors", func() {
		Context("when there are available Executors", func() {
			BeforeEach(func() {
				interval = 1 * time.Second

				firstPresence, status, err = bbs.MaintainExecutorPresence(interval, firstExecutorPresence)
				Ω(err).ShouldNot(HaveOccurred())
				Eventually(test_helpers.NewStatusReporter(status).Locked).Should(BeTrue())

				secondPresence, status, err = bbs.MaintainExecutorPresence(interval, secondExecutorPresence)
				Ω(err).ShouldNot(HaveOccurred())
				Eventually(test_helpers.NewStatusReporter(status).Locked).Should(BeTrue())
			})

			AfterEach(func() {
				firstPresence.Remove()
				secondPresence.Remove()
			})

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
			It("should return empty", func() {
				reps, err := bbs.GetAllExecutors()
				Ω(err).ShouldNot(HaveOccurred())
				Ω(reps).Should(BeEmpty())
			})
		})
	})
})
