package main_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/auction/communication/http/auction_http_client"
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/cmd/rep/testrunner"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/storeadapter"
	"github.com/pivotal-golang/clock"
	"github.com/pivotal-golang/lager/lagertest"

	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
	"github.com/onsi/gomega/ghttp"
)

var runner *testrunner.Runner
var etcdAdapter storeadapter.StoreAdapter

var _ = Describe("The Rep", func() {
	var (
		fakeExecutor      *ghttp.Server
		bbs               *Bbs.BBS
		pollingInterval   time.Duration
		evacuationTimeout time.Duration
		rootFSName        string
		rootFSPath        string
		logger            *lagertest.TestLogger

		flushEvents chan struct{}
	)

	BeforeEach(func() {
		flushEvents = make(chan struct{})
		fakeExecutor = ghttp.NewServer()
		// these tests only look for the start of a sequence of requests
		fakeExecutor.AllowUnhandledRequests = true
		fakeExecutor.RouteToHandler("GET", "/ping", ghttp.RespondWith(http.StatusOK, nil))
		fakeExecutor.RouteToHandler("GET", "/resources/total", func(w http.ResponseWriter, r *http.Request) {
			jsonBytes, err := json.Marshal(executor.ExecutorResources{
				MemoryMB:   512,
				DiskMB:     1024,
				Containers: 128,
			})
			Expect(err).NotTo(HaveOccurred())
			w.Write(jsonBytes)
		})
		fakeExecutor.RouteToHandler("GET", "/events", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			// keep event stream from terminating
			<-flushEvents
		})
		fakeExecutor.RouteToHandler(
			"GET",
			"/containers",
			ghttp.RespondWith(http.StatusOK, "[]"),
		)

		etcdAdapter = etcdRunner.Adapter()
		logger = lagertest.NewTestLogger("test")
		receptorTaskHandlerURL := "http://receptor.bogus.com"
		bbs = Bbs.NewBBS(etcdAdapter, consulSession, receptorTaskHandlerURL, clock.NewClock(), logger)

		pollingInterval = 50 * time.Millisecond
		evacuationTimeout = 200 * time.Millisecond

		rootFSName = "the-rootfs"
		rootFSPath = "/path/to/rootfs"
		rootFSArg := fmt.Sprintf("%s:%s", rootFSName, rootFSPath)

		runner = testrunner.New(
			representativePath,
			cellID,
			fakeExecutor.URL(),
			fmt.Sprintf("http://127.0.0.1:%d", etcdPort),
			consulRunner.ConsulCluster(),
			receptorTaskHandlerURL,
			"info",
			[]string{rootFSArg},
			[]string{"docker"},
			serverPort,
			pollingInterval,
			evacuationTimeout,
		)
	})

	JustBeforeEach(func() {
		runner.Start()
	})

	AfterEach(func(done Done) {
		close(flushEvents)
		etcdAdapter.Disconnect()
		runner.KillWithFire()
		fakeExecutor.Close()
		close(done)
	})

	Describe("when an interrupt signal is sent to the representative", func() {
		JustBeforeEach(func() {
			runner.Stop()
		})

		It("should die", func() {
			Eventually(runner.Session.ExitCode).Should(Equal(0))
		})
	})

	Context("when etcd is down", func() {
		BeforeEach(func() {
			etcdRunner.Stop()
		})

		AfterEach(func() {
			etcdRunner.Start()
		})

		It("starts", func() {
			Consistently(runner.Session).ShouldNot(Exit())
		})
	})

	Describe("maintaining presence", func() {
		var cellPresence models.CellPresence

		JustBeforeEach(func() {
			Eventually(bbs.Cells).Should(HaveLen(1))
			cells, err := bbs.Cells()
			Expect(err).NotTo(HaveOccurred())
			cellPresence = cells[0]
		})

		It("should maintain presence", func() {
			Expect(cellPresence.CellID).To(Equal(cellID))
		})

		Context("when the presence fails to be maintained", func() {
			It("should not exit, but keep trying to maintain presence at the same ID", func() {
				consulRunner.Reset()

				Eventually(bbs.Cells, 5).Should(HaveLen(1))
				cells, err := bbs.Cells()
				Expect(err).NotTo(HaveOccurred())
				Expect(cells[0]).To(Equal(cellPresence))

				Expect(runner.Session).NotTo(Exit())
			})
		})
	})

	Describe("acting as an auction representative", func() {
		Describe("reporting the state of its resources", func() {
			JustBeforeEach(func() {
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
				Expect(err).NotTo(HaveOccurred())

				client := auction_http_client.New(http.DefaultClient, cells[0].CellID, cells[0].RepAddress, lagertest.NewTestLogger("auction-client"))

				state, err := client.State()
				Expect(err).NotTo(HaveOccurred())
				Expect(state.TotalResources).To(Equal(auctiontypes.Resources{
					MemoryMB:   1024,
					DiskMB:     2048,
					Containers: 4,
				}))

				Expect(state.AvailableResources).To(Equal(auctiontypes.Resources{
					MemoryMB:   512,
					DiskMB:     1024,
					Containers: 2,
				}))

			})
		})

		Describe("performing work", func() {
			Describe("starting LRPs", func() {
				var desiredLRP models.DesiredLRP
				var index int
				var gotReservation chan struct{}

				JustBeforeEach(func() {
					index = 1
					desiredLRP = models.DesiredLRP{
						ProcessGuid: "the-process-guid",
						MemoryMB:    2,
						DiskMB:      2,
						RootFS:      "the:rootfs",
						Domain:      "the-domain",
						Instances:   index + 1,
						Action: &models.RunAction{
							Path: "the-path",
							Args: []string{},
						},
					}

					gotReservation = make(chan struct{})

					err := bbs.DesireLRP(logger, desiredLRP)
					Expect(err).NotTo(HaveOccurred())

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
					Expect(err).NotTo(HaveOccurred())

					client := auction_http_client.New(http.DefaultClient, cells[0].CellID, cells[0].RepAddress, lagertest.NewTestLogger("auction-client"))

					works := auctiontypes.Work{
						LRPs: []auctiontypes.LRPAuction{{
							DesiredLRP: desiredLRP,
							Index:      index,
						}},
					}

					failedWorks, err := client.Perform(works)
					Expect(err).NotTo(HaveOccurred())
					Expect(failedWorks.LRPs).To(BeEmpty())

					Eventually(gotReservation).Should(BeClosed())
				})
			})

			Describe("running tasks", func() {
				var task models.Task
				var gotReservation chan struct{}

				JustBeforeEach(func() {
					task = models.Task{
						TaskGuid: "the-task-guid",
						MemoryMB: 2,
						DiskMB:   2,
						RootFS:   "the:rootfs",
						Domain:   "the-domain",
						Action: &models.RunAction{
							Path: "date",
						},
					}

					gotReservation = make(chan struct{})

					err := bbs.DesireTask(logger, task)
					Expect(err).NotTo(HaveOccurred())

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
					Expect(err).NotTo(HaveOccurred())

					client := auction_http_client.New(http.DefaultClient, cells[0].CellID, cells[0].RepAddress, lagertest.NewTestLogger("auction-client"))

					Expect(bbs.PendingTasks(logger)).To(HaveLen(1))
					Expect(bbs.RunningTasks(logger)).To(BeEmpty())

					works := auctiontypes.Work{
						Tasks: []models.Task{task},
					}

					failedWorks, err := client.Perform(works)
					Expect(err).NotTo(HaveOccurred())
					Expect(failedWorks.Tasks).To(BeEmpty())

					Eventually(gotReservation).Should(BeClosed())
				})
			})
		})
	})

	Describe("polling the BBS for tasks to reap", func() {
		var task models.Task

		JustBeforeEach(func() {
			task = models.Task{
				TaskGuid: "a-new-task-guid",
				Domain:   "the-domain",
				RootFS:   "some:rootfs",
				Action: &models.RunAction{
					Path: "the-path",
					Args: []string{},
				},
			}

			err := bbs.DesireTask(logger, task)
			Expect(err).NotTo(HaveOccurred())

			_, err = bbs.StartTask(logger, task.TaskGuid, cellID)
			Expect(err).NotTo(HaveOccurred())
		})

		It("eventually marks tasks with no corresponding container as failed", func() {
			Eventually(func() ([]models.Task, error) {
				return bbs.CompletedTasks(logger)
			}, 5*pollingInterval).Should(HaveLen(1))

			completedTasks, err := bbs.CompletedTasks(logger)
			Expect(err).NotTo(HaveOccurred())

			Expect(completedTasks[0].TaskGuid).To(Equal(task.TaskGuid))
			Expect(completedTasks[0].Failed).To(BeTrue())
		})
	})

	Describe("polling the BBS for actual LRPs to reap", func() {
		JustBeforeEach(func() {
			desiredLRP := models.DesiredLRP{
				ProcessGuid: "process-guid",
				RootFS:      "some:rootfs",
				Domain:      "some-domain",
				Instances:   1,
				Action: &models.RunAction{
					Path: "the-path",
					Args: []string{},
				},
			}
			index := 0

			err := bbs.DesireLRP(logger, desiredLRP)
			Expect(err).NotTo(HaveOccurred())

			actualLRPGroup, err := bbs.ActualLRPGroupByProcessGuidAndIndex(desiredLRP.ProcessGuid, index)
			Expect(err).NotTo(HaveOccurred())

			instanceKey := models.NewActualLRPInstanceKey("some-instance-guid", cellID)
			err = bbs.ClaimActualLRP(logger, actualLRPGroup.Instance.ActualLRPKey, instanceKey)
			Expect(err).NotTo(HaveOccurred())
		})

		It("eventually reaps actual LRPs with no corresponding container", func() {
			Eventually(bbs.ActualLRPs, 5*pollingInterval).Should(BeEmpty())
		})
	})

	Describe("when a StopLRPInstance request comes in", func() {
		const processGuid = "process-guid"
		const instanceGuid = "some-instance-guid"
		var runningLRP models.ActualLRP
		var containerGuid = rep.LRPContainerGuid(processGuid, instanceGuid)
		var expectedStopRoute = "/containers/" + containerGuid + "/stop"

		JustBeforeEach(func() {
			fakeExecutor.RouteToHandler(
				"POST",
				expectedStopRoute,
				ghttp.RespondWith(http.StatusOK, nil),
			)

			// ensure the container remains after being stopped
			fakeExecutor.RouteToHandler(
				"GET",
				"/containers",
				ghttp.RespondWithJSONEncoded(http.StatusOK, []executor.Container{
					{
						Guid:  containerGuid,
						State: executor.StateCompleted,
					},
				}),
			)

			lrpKey := models.NewActualLRPKey(processGuid, 1, "domain")
			instanceKey := models.NewActualLRPInstanceKey(instanceGuid, cellID)
			netInfo := models.NewActualLRPNetInfo("bogus-ip", []models.PortMapping{})

			err := bbs.StartActualLRP(logger, lrpKey, instanceKey, netInfo)
			Expect(err).NotTo(HaveOccurred())

			lrpGroup, err := bbs.ActualLRPGroupByProcessGuidAndIndex(lrpKey.ProcessGuid, lrpKey.Index)
			Expect(err).NotTo(HaveOccurred())
			runningLRP = *lrpGroup.Instance
		})

		It("should stop the container", func() {
			bbs.RetireActualLRPs(logger, []models.ActualLRPKey{runningLRP.ActualLRPKey})

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

	Describe("cancelling tasks", func() {
		const taskGuid = "some-task-guid"
		const expectedDeleteRoute = "/containers/" + taskGuid

		var deletedContainer chan struct{}

		JustBeforeEach(func() {
			deletedContainer = make(chan struct{})

			fakeExecutor.RouteToHandler(
				"DELETE",
				expectedDeleteRoute,
				ghttp.CombineHandlers(
					func(http.ResponseWriter, *http.Request) {
						close(deletedContainer)
					},
					ghttp.RespondWith(http.StatusOK, nil),
				),
			)

			task := models.Task{
				TaskGuid: taskGuid,
				Domain:   "the-domain",
				RootFS:   "some:rootfs",
				Action: &models.RunAction{
					Path: "date",
				},
			}

			err := bbs.DesireTask(logger, task)
			Expect(err).NotTo(HaveOccurred())

			started, err := bbs.StartTask(logger, taskGuid, cellID)
			Expect(err).NotTo(HaveOccurred())
			Expect(started).To(BeTrue())
		})

		It("deletes the container", func() {
			err := bbs.CancelTask(logger, taskGuid)
			Expect(err).NotTo(HaveOccurred())

			Eventually(deletedContainer).Should(BeClosed())

			Consistently(func() ([]models.Task, error) {
				return bbs.Tasks(logger)
			}).Should(HaveLen(1))
		})
	})

	Describe("Evacuation", func() {
		Context("when it has running LRP containers", func() {
			var (
				processGuid  string
				index        int
				domain       string
				instanceGuid string
				address      string

				lrpKey          models.ActualLRPKey
				lrpContainerKey models.ActualLRPInstanceKey
				lrpNetInfo      models.ActualLRPNetInfo
			)

			JustBeforeEach(func() {
				processGuid = "some-process-guid"
				index = 2
				domain = "some-domain"

				instanceGuid = "some-instance-guid"
				address = "some-external-ip"

				lrpKey = models.NewActualLRPKey(processGuid, index, domain)
				lrpContainerKey = models.NewActualLRPInstanceKey(instanceGuid, cellID)
				lrpNetInfo = models.NewActualLRPNetInfo(address, []models.PortMapping{{ContainerPort: 1470, HostPort: 2589}})

				err := bbs.StartActualLRP(logger, lrpKey, lrpContainerKey, lrpNetInfo)
				Expect(err).NotTo(HaveOccurred())

				container := executor.Container{
					Guid:       rep.LRPContainerGuid(processGuid, instanceGuid),
					State:      executor.StateRunning,
					Action:     &models.RunAction{Path: "true"},
					ExternalIP: address,
					Ports:      []executor.PortMapping{{ContainerPort: 1470, HostPort: 2589}},
					Tags: executor.Tags{
						rep.LifecycleTag:    rep.LRPLifecycle,
						rep.ProcessGuidTag:  processGuid,
						rep.InstanceGuidTag: instanceGuid,
						rep.ProcessIndexTag: strconv.Itoa(index),
						rep.DomainTag:       domain,
					},
				}
				containers := []executor.Container{container}

				fakeExecutor.RouteToHandler(
					"GET",
					"/containers",
					ghttp.RespondWithJSONEncoded(http.StatusOK, containers),
				)
				fakeExecutor.RouteToHandler(
					"GET",
					"/containers/"+container.Guid,
					ghttp.RespondWithJSONEncoded(http.StatusOK, container),
				)

				resp, err := http.Post(fmt.Sprintf("http://0.0.0.0:%d/evacuate", serverPort), "text/html", nil)
				Expect(err).NotTo(HaveOccurred())
				resp.Body.Close()
				Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
			})

			It("evacuates them", func() {
				var actualLRP models.ActualLRP

				getEvacuatingLRP := func() *models.ActualLRP {
					node, err := etcdAdapter.Get(shared.EvacuatingActualLRPSchemaPath(processGuid, index))
					if err != nil {
						return nil
					}
					err = json.Unmarshal([]byte(node.Value), &actualLRP)
					Expect(err).NotTo(HaveOccurred())

					return &actualLRP
				}

				Eventually(getEvacuatingLRP, 1).ShouldNot(BeNil())
				Expect(actualLRP.ProcessGuid).To(Equal(processGuid))
			})

			Context("when exceeding the evacuation timeout", func() {
				It("shuts down gracefully", func() {
					// wait longer than expected to let OS and Go runtime reap process
					Eventually(runner.Session.ExitCode, 2*evacuationTimeout+2*time.Second).Should(Equal(0))
				})
			})

			Context("when signaled to stop", func() {
				JustBeforeEach(func() {
					runner.Stop()
				})

				It("shuts down gracefully", func() {
					Eventually(runner.Session.ExitCode).Should(Equal(0))
				})
			})
		})
	})

	Describe("when a Ping request comes in", func() {
		It("responds with 200 OK", func() {
			resp, err := http.Get(fmt.Sprintf("http://0.0.0.0:%d/ping", serverPort))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
		})
	})
})
