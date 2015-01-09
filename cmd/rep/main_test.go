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
	"github.com/cloudfoundry/storeadapter"
	"github.com/pivotal-golang/lager/lagertest"

	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/onsi/gomega/ghttp"
)

var runner *testrunner.Runner
var etcdAdapter storeadapter.StoreAdapter

var _ = Describe("The Rep", func() {
	var (
		fakeExecutor    *ghttp.Server
		bbs             *Bbs.BBS
		pollingInterval time.Duration
		logger          *lagertest.TestLogger
	)

	BeforeEach(func() {
		fakeExecutor = ghttp.NewServer()
		// these tests only look for the start of a sequence of requests
		fakeExecutor.AllowUnhandledRequests = true
		fakeExecutor.RouteToHandler("GET", "/ping", ghttp.RespondWith(http.StatusOK, nil))

		etcdAdapter = etcdRunner.Adapter()
		logger = lagertest.NewTestLogger("test")
		bbs = Bbs.NewBBS(etcdAdapter, timeprovider.NewTimeProvider(), logger)

		pollingInterval = 50 * time.Millisecond

		runner = testrunner.New(
			representativePath,
			cellID,
			"the-stack",
			fakeExecutor.URL(),
			fmt.Sprintf("http://127.0.0.1:%d", etcdPort),
			"info",
			serverPort,
			time.Second,
			pollingInterval,
		)

		runner.Start()
	})

	AfterEach(func(done Done) {
		etcdAdapter.Disconnect()
		runner.KillWithFire()
		fakeExecutor.Close()
		close(done)
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

			bbs.DesireTask(logger, models.Task{
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
		Describe("reporting the state of its resources", func() {
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

		Describe("performing work", func() {
			Describe("starting LRPs", func() {
				var desiredLRP models.DesiredLRP
				var index int
				var gotReservation chan struct{}

				BeforeEach(func() {
					index = 1
					desiredLRP = models.DesiredLRP{
						ProcessGuid: "the-process-guid",
						MemoryMB:    2,
						DiskMB:      2,
						Stack:       "the-stack",
						Domain:      "the-domain",
						Instances:   index + 1,
					}

					gotReservation = make(chan struct{})

					err := bbs.CreateActualLRP(desiredLRP, index, logger)
					Ω(err).ShouldNot(HaveOccurred())

					fakeExecutor.RouteToHandler("POST", "/containers",
						ghttp.CombineHandlers(
							ghttp.RespondWithJSONEncoded(http.StatusOK, map[string]string{}),
							func(w http.ResponseWriter, r *http.Request) {
								close(gotReservation)
							},
						),
					)

				})

				It("makes a request to executor to allocate the container", func() {
					Eventually(bbs.Cells).Should(HaveLen(1))
					cells, err := bbs.Cells()
					Ω(err).ShouldNot(HaveOccurred())

					client := auction_http_client.New(http.DefaultClient, cells[0].CellID, cells[0].RepAddress, lagertest.NewTestLogger("auction-client"))

					works := auctiontypes.Work{
						LRPs: []auctiontypes.LRPAuction{{
							DesiredLRP: desiredLRP,
							Index:      index,
						}},
					}

					failedWorks, err := client.Perform(works)
					Ω(err).ShouldNot(HaveOccurred())
					Ω(failedWorks.LRPs).Should(BeEmpty())

					Eventually(gotReservation).Should(BeClosed())
				})
			})

			Describe("running tasks", func() {
				var task models.Task
				var gotReservation chan struct{}

				BeforeEach(func() {
					task = models.Task{
						TaskGuid: "the-task-guid",
						MemoryMB: 2,
						DiskMB:   2,
						Stack:    "the-stack",
						Domain:   "the-domain",
						Action: &models.RunAction{
							Path: "date",
						},
					}

					gotReservation = make(chan struct{})

					err := bbs.DesireTask(logger, task)
					Ω(err).ShouldNot(HaveOccurred())

					fakeExecutor.RouteToHandler("POST", "/containers",
						ghttp.CombineHandlers(
							ghttp.RespondWithJSONEncoded(http.StatusOK, map[string]string{}),
							func(w http.ResponseWriter, r *http.Request) {
								close(gotReservation)
							},
						),
					)

				})

				It("makes a request to executor to allocate the container", func() {
					Eventually(bbs.Cells).Should(HaveLen(1))
					cells, err := bbs.Cells()
					Ω(err).ShouldNot(HaveOccurred())

					client := auction_http_client.New(http.DefaultClient, cells[0].CellID, cells[0].RepAddress, lagertest.NewTestLogger("auction-client"))

					Ω(bbs.PendingTasks(logger)).Should(HaveLen(1))
					Ω(bbs.RunningTasks(logger)).Should(BeEmpty())

					works := auctiontypes.Work{
						Tasks: []models.Task{task},
					}

					failedWorks, err := client.Perform(works)
					Ω(err).ShouldNot(HaveOccurred())
					Ω(failedWorks.Tasks).Should(BeEmpty())

					Eventually(gotReservation).Should(BeClosed())
				})
			})
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

			err := bbs.DesireTask(logger, task)
			Ω(err).ShouldNot(HaveOccurred())

			_, err = bbs.StartTask(logger, task.TaskGuid, cellID)
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("eventually marks tasks with no corresponding container as failed", func() {
			Eventually(func() ([]models.Task, error) {
				return bbs.CompletedTasks(logger)
			}, 5*pollingInterval).Should(HaveLen(1))

			completedTasks, err := bbs.CompletedTasks(logger)
			Ω(err).ShouldNot(HaveOccurred())

			Ω(completedTasks[0].TaskGuid).Should(Equal(task.TaskGuid))
			Ω(completedTasks[0].Failed).Should(BeTrue())
		})
	})

	Describe("polling the BBS for actual LRPs to reap", func() {
		BeforeEach(func() {
			fakeExecutor.RouteToHandler(
				"GET",
				"/containers",
				ghttp.RespondWith(http.StatusOK, "[]"),
			)

			desiredLRP := models.DesiredLRP{
				ProcessGuid: "process-guid",
				Domain:      "some-domain",
				Instances:   1,
			}
			index := 0

			err := bbs.CreateActualLRP(desiredLRP, index, logger)
			Ω(err).ShouldNot(HaveOccurred())

			actualLRP, err := bbs.ActualLRPByProcessGuidAndIndex(desiredLRP.ProcessGuid, index)
			Ω(err).ShouldNot(HaveOccurred())

			containerKey := models.NewActualLRPContainerKey("some-instance-guid", cellID)
			err = bbs.ClaimActualLRP(actualLRP.ActualLRPKey, containerKey, logger)
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("eventually reaps actual LRPs with no corresponding container", func() {
			Eventually(bbs.ActualLRPs, 5*pollingInterval).Should(BeEmpty())
		})
	})

	Describe("when a StopLRPInstance request comes in", func() {
		var runningLRP models.ActualLRP
		const instanceGuid = "some-instance-guid"
		const expectedStopRoute = "/containers/" + instanceGuid + "/stop"

		BeforeEach(func() {
			fakeExecutor.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("POST", expectedStopRoute),
					ghttp.RespondWith(http.StatusOK, nil),
				),
			)

			lrpKey := models.NewActualLRPKey("process-guid", 1, "domain")
			containerKey := models.NewActualLRPContainerKey(instanceGuid, cellID)
			netInfo := models.NewActualLRPNetInfo("bogus-ip", []models.PortMapping{})

			err := bbs.StartActualLRP(lrpKey, containerKey, netInfo, logger)
			Ω(err).ShouldNot(HaveOccurred())

			runningLRP, err = bbs.ActualLRPByProcessGuidAndIndex(lrpKey.ProcessGuid, lrpKey.Index)
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("should stop the container", func() {
			err := bbs.RequestStopLRPInstance(runningLRP.ActualLRPKey, runningLRP.ActualLRPContainerKey)
			Ω(err).ShouldNot(HaveOccurred())

			findStopRequest := func() bool {
				for _, req := range fakeExecutor.ReceivedRequests() {
					if req.URL.Path == expectedStopRoute {
						return true
					}
				}
				return false
			}

			Eventually(findStopRequest).Should(BeTrue())
			Consistently(bbs.ActualLRPs).Should(HaveLen(1))
		})
	})
})
