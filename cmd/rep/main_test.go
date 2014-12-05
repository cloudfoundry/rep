package main_test

import (
	"fmt"
	"net/http"
	"time"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/auction/communication/http/auction_http_client"
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
		fakeExecutor    *ghttp.Server
		bbs             *Bbs.BBS
		pollingInterval time.Duration
	)

	BeforeEach(func() {
		fakeExecutor = ghttp.NewServer()
		// these tests only look for the start of a sequence of requests
		fakeExecutor.AllowUnhandledRequests = true
		fakeExecutor.RouteToHandler("GET", "/ping", ghttp.RespondWith(http.StatusOK, nil))

		bbs = Bbs.NewBBS(etcdRunner.Adapter(), timeprovider.NewTimeProvider(), lagertest.NewTestLogger("test"))

		pollingInterval = 50 * time.Millisecond

		runner = testrunner.New(
			representativePath,
			cellID,
			"the-stack",
			"the-lrp-host",
			fakeExecutor.URL(),
			fmt.Sprintf("http://127.0.0.1:%d", etcdPort),
			"info",
			auctionServerPort,
			time.Second,
			pollingInterval,
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
			_, err := bbs.ReportActualLRPAsStarting("some-process-guid1", "some-instance-guid1", cellID, "domain", 0)
			Ω(err).ShouldNot(HaveOccurred())

			lrp2rep1, err := bbs.ReportActualLRPAsStarting("some-process-guid2", "some-instance-guid2", cellID, "domain", 0)
			Ω(err).ShouldNot(HaveOccurred())
			err = bbs.ReportActualLRPAsRunning(lrp2rep1, cellID)
			Ω(err).ShouldNot(HaveOccurred())

			_, err = bbs.ReportActualLRPAsStarting("some-process-guid3", "some-instance-guid3", "different-cell-id", "domain", 0)
			Ω(err).ShouldNot(HaveOccurred())

			lrp2rep2, err := bbs.ReportActualLRPAsStarting("some-process-guid4", "some-instance-guid4", "different-cell-id", "domain", 0)
			Ω(err).ShouldNot(HaveOccurred())
			err = bbs.ReportActualLRPAsRunning(lrp2rep2, "different-cell-id")
			Ω(err).ShouldNot(HaveOccurred())

			runner.Stop()
			actualLrpsForRep1 := func() ([]models.ActualLRP, error) {
				return bbs.ActualLRPsByCellID(cellID)
			}
			Consistently(actualLrpsForRep1).Should(HaveLen(2))
			actualLrpsForRep2 := func() ([]models.ActualLRP, error) {
				return bbs.ActualLRPsByCellID("different-cell-id")
			}
			Consistently(actualLrpsForRep2).Should(HaveLen(2))
		})

		JustBeforeEach(func() {
			runner.Start()
		})

		It("should delete its corresponding actual LRPs", func() {
			actualLrpsForRep1 := func() ([]models.ActualLRP, error) {
				return bbs.ActualLRPsByCellID(cellID)
			}
			Eventually(actualLrpsForRep1).Should(BeEmpty())

			actualLrpsForRep2 := func() ([]models.ActualLRP, error) {
				return bbs.ActualLRPsByCellID("different-cell-id")
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
		var cellPresence models.CellPresence

		BeforeEach(func() {
			Eventually(bbs.Cells).Should(HaveLen(1))
			cells, err := bbs.Cells()
			Ω(err).ShouldNot(HaveOccurred())
			cellPresence = cells[0]
		})

		It("should maintain presence", func() {
			Ω(cellPresence.Stack).Should(Equal("the-stack"))
			Ω(cellPresence.CellID).Should(Equal(cellID))
		})

		Context("when the presence fails to be maintained", func() {
			It("should not exit, but keep trying to maintain presence at the same ID", func() {
				etcdRunner.Stop()
				etcdRunner.Start()

				Eventually(bbs.Cells, 5).Should(HaveLen(1))
				cells, err := bbs.Cells()
				Ω(err).ShouldNot(HaveOccurred())
				Ω(cells[0]).Should(Equal(cellPresence))

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
				Action: &models.RunAction{
					Path: "cat",
					Args: []string{"/tmp/file"},
				},
			})
		})

		It("makes a request to the executor", func() {
			Eventually(fakeExecutor.ReceivedRequests).Should(HaveLen(2))
		})
	})

	Describe("acting as an auction representative", func() {
		BeforeEach(func() {
			fakeExecutor.RouteToHandler("GET", "/resources/total", ghttp.RespondWithJSONEncoded(http.StatusOK, executor.ExecutorResources{
				MemoryMB:   1024,
				DiskMB:     2048,
				Containers: 4,
			}))
			fakeExecutor.RouteToHandler("GET", "/resources/remaining", ghttp.RespondWithJSONEncoded(http.StatusOK, executor.ExecutorResources{
				MemoryMB:   512,
				DiskMB:     1024,
				Containers: 2,
			}))
			fakeExecutor.RouteToHandler("GET", "/containers", ghttp.RespondWithJSONEncoded(http.StatusOK, []executor.Container{}))
		})

		It("makes a request to the executor", func() {
			Eventually(bbs.Cells).Should(HaveLen(1))
			cells, err := bbs.Cells()
			Ω(err).ShouldNot(HaveOccurred())

			client := auction_http_client.New(http.DefaultClient, cells[0].CellID, cells[0].RepAddress, lagertest.NewTestLogger("auction-client"))

			state, err := client.State()
			Ω(err).ShouldNot(HaveOccurred())
			Ω(state.TotalResources).Should(Equal(auctiontypes.Resources{
				MemoryMB:   1024,
				DiskMB:     2048,
				Containers: 4,
			}))
			Ω(state.AvailableResources).Should(Equal(auctiontypes.Resources{
				MemoryMB:   512,
				DiskMB:     1024,
				Containers: 2,
			}))
			Ω(state.Stack).Should(Equal("the-stack"))
		})
	})

	Describe("polling the BBS for tasks to reap", func() {
		var task models.Task

		BeforeEach(func() {
			fakeExecutor.RouteToHandler(
				"GET",
				"/containers",
				ghttp.RespondWith(http.StatusOK, "[]"),
			)

			task = models.Task{
				TaskGuid: "a-new-task-guid",
				Domain:   "the-domain",
				Action: &models.RunAction{
					Path: "the-path",
					Args: []string{},
				},
				Stack: "the-stack",
			}

			err := bbs.DesireTask(task)
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.ClaimTask(task.TaskGuid, cellID)
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("eventually marks tasks with no corresponding container as failed", func() {
			Eventually(bbs.CompletedTasks, 5*pollingInterval).Should(HaveLen(1))

			completedTasks, err := bbs.CompletedTasks()
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
				"/containers",
				ghttp.RespondWith(http.StatusOK, "[]"),
			)

			var err error
			actualLRP, err = bbs.ReportActualLRPAsStarting("process-guid", "a-new-instance-guid", cellID, "the-domain", 0)
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("eventually reaps actual LRPs with no corresponding container", func() {
			Eventually(bbs.ActualLRPs, 5*pollingInterval).Should(BeEmpty())
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

			lrp := models.ActualLRP{
				ProcessGuid:  "some-process-guid",
				InstanceGuid: "some-instance-guid",
				Domain:       "the-domain",
				Index:        3,

				CellID: cellID,
			}

			err := bbs.ReportActualLRPAsRunning(lrp, cellID)
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.RequestStopLRPInstance(lrp)
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("should delete the container and resolve the StopLRPInstance", func() {
			findDeleteRequest := func() string {
				for _, req := range fakeExecutor.ReceivedRequests() {
					if req.Method == "DELETE" {
						return req.URL.Path
					}
				}
				return ""
			}

			Eventually(findDeleteRequest).Should(Equal("/containers/some-instance-guid"))
			Eventually(bbs.ActualLRPs).Should(BeEmpty())
		})
	})
})
