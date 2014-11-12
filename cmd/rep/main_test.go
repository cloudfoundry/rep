package main_test

import (
	"fmt"
	"net/http"
	"time"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/auction/communication/nats/auction_nats_client"
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep/cmd/rep/testrunner"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/pivotal-golang/lager/lagertest"

	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/onsi/gomega/ghttp"
)

var runner *testrunner.Runner

var _ = Describe("The Rep", func() {
	var (
		fakeExecutor             *ghttp.Server
		bbs                      *Bbs.BBS
		actualLRPReapingInterval time.Duration
		taskReapingInterval      time.Duration
	)

	BeforeEach(func() {
		fakeExecutor = ghttp.NewServer()
		// these tests only look for the start of a sequence of requests
		fakeExecutor.AllowUnhandledRequests = true
		fakeExecutor.RouteToHandler("GET", "/ping", ghttp.RespondWith(http.StatusOK, nil))

		bbs = Bbs.NewBBS(etcdRunner.Adapter(), timeprovider.NewTimeProvider(), lagertest.NewTestLogger("test"))

		actualLRPReapingInterval = 50 * time.Millisecond
		taskReapingInterval = 50 * time.Millisecond

		runner = testrunner.New(
			representativePath,
			executorID,
			"the-stack",
			"the-lrp-host",
			fakeExecutor.URL(),
			fmt.Sprintf("http://127.0.0.1:%d", etcdPort),
			fmt.Sprintf("127.0.0.1:%d", natsPort),
			"info",
			time.Second,
			actualLRPReapingInterval,
			taskReapingInterval,
		)

		runner.Start()
	})

	AfterEach(func(done Done) {
		runner.KillWithFire()
		fakeExecutor.Close()
		close(done)
	})

	Describe("when a rep starts up", func() {
		BeforeEach(func() {
			_, err := bbs.ReportActualLRPAsStarting("some-process-guid1", "some-instance-guid1", executorID, "domain", 0)
			Ω(err).ShouldNot(HaveOccurred())

			lrp2rep1, err := bbs.ReportActualLRPAsStarting("some-process-guid2", "some-instance-guid2", executorID, "domain", 0)
			Ω(err).ShouldNot(HaveOccurred())
			err = bbs.ReportActualLRPAsRunning(lrp2rep1, executorID)
			Ω(err).ShouldNot(HaveOccurred())

			_, err = bbs.ReportActualLRPAsStarting("some-process-guid3", "some-instance-guid3", "different-executor-id", "domain", 0)
			Ω(err).ShouldNot(HaveOccurred())

			lrp2rep2, err := bbs.ReportActualLRPAsStarting("some-process-guid4", "some-instance-guid4", "different-executor-id", "domain", 0)
			Ω(err).ShouldNot(HaveOccurred())
			err = bbs.ReportActualLRPAsRunning(lrp2rep2, "different-executor-id")
			Ω(err).ShouldNot(HaveOccurred())

			runner.Stop()
			actualLrpsForRep1 := func() ([]models.ActualLRP, error) {
				return bbs.GetAllActualLRPsByExecutorID(executorID)
			}
			Consistently(actualLrpsForRep1).Should(HaveLen(2))
			actualLrpsForRep2 := func() ([]models.ActualLRP, error) {
				return bbs.GetAllActualLRPsByExecutorID("different-executor-id")
			}
			Consistently(actualLrpsForRep2).Should(HaveLen(2))
		})

		JustBeforeEach(func() {
			runner.Start()
		})

		It("should delete its corresponding actual LRPs", func() {
			actualLrpsForRep1 := func() ([]models.ActualLRP, error) {
				return bbs.GetAllActualLRPsByExecutorID(executorID)
			}
			Eventually(actualLrpsForRep1).Should(BeEmpty())

			actualLrpsForRep2 := func() ([]models.ActualLRP, error) {
				return bbs.GetAllActualLRPsByExecutorID("different-executor-id")
			}
			Consistently(actualLrpsForRep2).Should(HaveLen(2))
		})
	})

	Describe("when an interrupt signal is sent to the representative", func() {
		BeforeEach(func() {
			runner.Stop()
		})

		It("should die", func() {
			Eventually(runner.Session.ExitCode).Should(Equal(0))
		})
	})

	Describe("maintaining presence", func() {
		var executorPresence models.ExecutorPresence

		BeforeEach(func() {
			Eventually(bbs.GetAllExecutors).Should(HaveLen(1))
			executors, err := bbs.GetAllExecutors()
			Ω(err).ShouldNot(HaveOccurred())
			executorPresence = executors[0]
		})

		It("should maintain presence", func() {
			Ω(executorPresence.Stack).Should(Equal("the-stack"))
			Ω(executorPresence.ExecutorID).Should(Equal(executorID))
		})

		Context("when the presence fails to be maintained", func() {
			It("should not exit, but keep trying to maintain presence at the same ID", func() {
				etcdRunner.Stop()
				etcdRunner.Start()

				Eventually(bbs.GetAllExecutors, 5).Should(HaveLen(1))
				executors, err := bbs.GetAllExecutors()
				Ω(err).ShouldNot(HaveOccurred())
				Ω(executors[0]).Should(Equal(executorPresence))

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
				TaskGuid: "the-task-guid",
				Stack:    "the-stack",
				Action: models.ExecutorAction{
					Action: models.RunAction{
						Path: "cat",
						Args: []string{"/tmp/file"},
					},
				},
			})
		})

		It("makes a request to the executor", func() {
			Eventually(fakeExecutor.ReceivedRequests).Should(HaveLen(2))
		})
	})

	Describe("acting as an auction representative", func() {
		BeforeEach(func() {
			fakeExecutor.AppendHandlers(ghttp.CombineHandlers(
				ghttp.VerifyRequest("GET", "/resources/total"),
				ghttp.RespondWithJSONEncoded(http.StatusOK, executor.ExecutorResources{
					MemoryMB:   1024,
					DiskMB:     2048,
					Containers: 4,
				})),
			)
		})

		It("makes a request to the executor", func() {
			Eventually(bbs.GetAllExecutors).Should(HaveLen(1))
			executors, err := bbs.GetAllExecutors()
			Ω(err).ShouldNot(HaveOccurred())
			executorID := executors[0].ExecutorID

			client, err := auction_nats_client.New(natsClient, time.Second, lagertest.NewTestLogger("test"))
			Ω(err).ShouldNot(HaveOccurred())
			resources := client.TotalResources(executorID)
			Ω(resources).Should(Equal(auctiontypes.Resources{
				MemoryMB:   1024,
				DiskMB:     2048,
				Containers: 4,
			}))
		})
	})

	Describe("polling the BBS for tasks to reap", func() {
		var task models.Task

		BeforeEach(func() {
			fakeExecutor.RouteToHandler(
				"GET",
				"/containers/a-new-task-guid",
				ghttp.RespondWith(http.StatusNotFound, "", http.Header{"X-Executor-Error": []string{"ContainerNotFound"}}),
			)

			task = models.Task{
				TaskGuid: "a-new-task-guid",
				Domain:   "the-domain",
				Action: models.ExecutorAction{
					Action: models.RunAction{
						Path: "the-path",
						Args: []string{},
					},
				},
				Stack: "the-stack",
			}

			err := bbs.DesireTask(task)
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.ClaimTask(task.TaskGuid, executorID)
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("eventually marks tasks with no corresponding container as failed", func() {
			Eventually(bbs.GetAllCompletedTasks, 5*taskReapingInterval).Should(HaveLen(1))

			completedTasks, err := bbs.GetAllCompletedTasks()
			Ω(err).ShouldNot(HaveOccurred())

			Ω(completedTasks[0].TaskGuid).Should(Equal(task.TaskGuid))
			Ω(completedTasks[0].Failed).Should(BeTrue())
		})
	})

	Describe("polling the BBS for actual LRPs to reap", func() {
		var actualLRP models.ActualLRP

		BeforeEach(func() {
			fakeExecutor.RouteToHandler(
				"GET",
				"/containers/a-new-instance-guid",
				ghttp.RespondWith(http.StatusNotFound, "", http.Header{"X-Executor-Error": []string{"ContainerNotFound"}}),
			)

			var err error
			actualLRP, err = bbs.ReportActualLRPAsStarting("process-guid", "a-new-instance-guid", executorID, "the-domain", 0)
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("eventually reaps actual LRPs with no corresponding container", func() {
			Eventually(bbs.GetAllActualLRPs, 5*actualLRPReapingInterval).Should(BeEmpty())
		})
	})

	Describe("when a StopLRPInstance request comes in", func() {
		BeforeEach(func() {
			fakeExecutor.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("DELETE", "/containers/some-instance-guid"),
					ghttp.RespondWith(http.StatusOK, nil),
				),
			)

			err := bbs.ReportActualLRPAsRunning(models.ActualLRP{
				ProcessGuid:  "some-process-guid",
				InstanceGuid: "some-instance-guid",
				Domain:       "the-domain",
				Index:        3,
			}, executorID)
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.RequestStopLRPInstance(models.StopLRPInstance{
				ProcessGuid:  "some-process-guid",
				InstanceGuid: "some-instance-guid",
				Index:        3,
			})
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("should delete the container and resolve the StopLRPInstance", func() {
			Eventually(fakeExecutor.ReceivedRequests).Should(HaveLen(4))
			Eventually(bbs.GetAllActualLRPs).Should(BeEmpty())
			Eventually(bbs.GetAllStopLRPInstances).Should(BeEmpty())
		})
	})
})
