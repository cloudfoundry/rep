package main_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"runtime"
	"time"

	"github.com/cloudfoundry-incubator/bbs"
	bbstestrunner "github.com/cloudfoundry-incubator/bbs/cmd/bbs/testrunner"
	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/bbs/models/test/model_helpers"
	"github.com/cloudfoundry-incubator/cf_http"
	"github.com/cloudfoundry-incubator/executor/gardenhealth"
	"github.com/cloudfoundry-incubator/garden"
	"github.com/cloudfoundry-incubator/garden/transport"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/cmd/rep/testrunner"
	"github.com/hashicorp/consul/api"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit/ginkgomon"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	. "github.com/onsi/gomega/gexec"
	"github.com/onsi/gomega/ghttp"
)

var runner *testrunner.Runner

var _ = Describe("The Rep", func() {
	var (
		fakeGarden        *ghttp.Server
		pollingInterval   time.Duration
		evacuationTimeout time.Duration
		rootFSName        string
		rootFSPath        string
		logger            *lagertest.TestLogger

		flushEvents chan struct{}
	)

	var getActualLRPGroups = func() []*models.ActualLRPGroup {
		actualLRPGroups, err := bbsClient.ActualLRPGroups(models.ActualLRPFilter{})
		Expect(err).NotTo(HaveOccurred())
		return actualLRPGroups
	}

	BeforeEach(func() {
		Eventually(getActualLRPGroups, 5*pollingInterval).Should(BeEmpty())
		flushEvents = make(chan struct{})
		fakeGarden = ghttp.NewUnstartedServer()
		// these tests only look for the start of a sequence of requests
		fakeGarden.AllowUnhandledRequests = false
		fakeGarden.RouteToHandler("GET", "/ping", ghttp.RespondWithJSONEncoded(http.StatusOK, struct{}{}))
		fakeGarden.RouteToHandler("GET", "/containers", ghttp.RespondWithJSONEncoded(http.StatusOK, struct{}{}))
		fakeGarden.RouteToHandler("GET", "/capacity", ghttp.RespondWithJSONEncoded(http.StatusOK,
			garden.Capacity{MemoryInBytes: 1024 * 1024 * 1024, DiskInBytes: 2048 * 1024 * 1024, MaxContainers: 4}))
		fakeGarden.RouteToHandler("GET", "/containers/bulk_info", ghttp.RespondWithJSONEncoded(http.StatusOK, struct{}{}))

		// The following handlers are needed to fake out the healthcheck containers
		fakeGarden.RouteToHandler("POST", "/containers", ghttp.RespondWithJSONEncoded(http.StatusOK, map[string]string{"handle": "healthcheck-container"}))
		fakeGarden.RouteToHandler("DELETE", regexp.MustCompile("/containers/executor-healthcheck-[-a-f0-9]+"), ghttp.RespondWithJSONEncoded(http.StatusOK, struct{}{}))
		fakeGarden.RouteToHandler("POST", "/containers/healthcheck-container/processes", func() http.HandlerFunc {
			firstResponse, err := json.Marshal(transport.ProcessPayload{})
			Expect(err).NotTo(HaveOccurred())

			exitStatus := 0
			secondResponse, err := json.Marshal(transport.ProcessPayload{ExitStatus: &exitStatus})
			Expect(err).NotTo(HaveOccurred())

			headers := http.Header{"Content-Type": []string{"application/json"}}
			response := string(firstResponse) + string(secondResponse)
			return ghttp.RespondWith(http.StatusOK, response, headers)
		}())

		logger = lagertest.NewTestLogger("test")

		pollingInterval = 50 * time.Millisecond
		evacuationTimeout = 200 * time.Millisecond

		rootFSName = "the-rootfs"
		rootFSPath = "/path/to/rootfs"
		rootFSArg := fmt.Sprintf("%s:%s", rootFSName, rootFSPath)

		runner = testrunner.New(
			representativePath,
			testrunner.Config{
				PreloadedRootFSes: []string{rootFSArg},
				RootFSProviders:   []string{"docker"},
				CellID:            cellID,
				BBSAddress:        bbsURL.String(),
				ServerPort:        serverPort,
				GardenAddr:        fakeGarden.HTTPTestServer.Listener.Addr().String(),
				LogLevel:          "debug",
				ConsulCluster:     consulRunner.ConsulCluster(),
				PollingInterval:   pollingInterval,
				EvacuationTimeout: evacuationTimeout,
			},
		)
	})

	JustBeforeEach(func() {
		runner.Start()
	})

	AfterEach(func(done Done) {
		close(flushEvents)
		runner.KillWithFire()
		fakeGarden.Close()
		close(done)
	})

	Context("when Garden is available", func() {
		BeforeEach(func() {
			fakeGarden.Start()
		})

		Describe("when an interrupt signal is sent to the representative", func() {
			JustBeforeEach(func() {
				if runtime.GOOS == "windows" {
					Skip("Interrupt isn't supported on windows")
				}

				runner.Stop()
			})

			It("should die", func() {
				Eventually(runner.Session.ExitCode).Should(Equal(0))
			})
		})

		Context("when etcd is down", func() {
			BeforeEach(func() {
				etcdRunner.KillWithFire()
			})

			AfterEach(func() {
				etcdRunner.Start()
			})

			It("starts", func() {
				Consistently(runner.Session).ShouldNot(Exit())
			})
		})

		Context("when starting", func() {
			var deleteChan chan struct{}
			BeforeEach(func() {
				fakeGarden.RouteToHandler("GET", "/containers",
					func(w http.ResponseWriter, r *http.Request) {
						r.ParseForm()
						healthcheckTagQueryParam := gardenhealth.HealthcheckTag
						if r.FormValue(healthcheckTagQueryParam) == gardenhealth.HealthcheckTagValue {
							ghttp.RespondWithJSONEncoded(http.StatusOK, struct{}{})(w, r)
						} else {
							ghttp.RespondWithJSONEncoded(http.StatusOK, map[string][]string{"handles": []string{"cnr1", "cnr2"}})(w, r)
						}
					},
				)
				deleteChan = make(chan struct{}, 2)
				fakeGarden.RouteToHandler("DELETE", "/containers/cnr1",
					ghttp.CombineHandlers(
						func(http.ResponseWriter, *http.Request) {
							deleteChan <- struct{}{}
						},
						ghttp.RespondWithJSONEncoded(http.StatusOK, &struct{}{})))
				fakeGarden.RouteToHandler("DELETE", "/containers/cnr2",
					ghttp.CombineHandlers(
						func(http.ResponseWriter, *http.Request) {
							deleteChan <- struct{}{}
						},
						ghttp.RespondWithJSONEncoded(http.StatusOK, &struct{}{})))
			})

			It("destroys any existing containers", func() {
				Eventually(deleteChan).Should(Receive())
				Eventually(deleteChan).Should(Receive())
			})
		})

		Describe("maintaining presence", func() {
			var cellPresence *models.CellPresence

			JustBeforeEach(func() {
				Eventually(fetchCells(logger)).Should(HaveLen(1))
				cells, err := bbsClient.Cells()
				cellSet := models.NewCellSetFromList(cells)
				Expect(err).NotTo(HaveOccurred())
				cellPresence = cellSet[cellID]
			})

			It("should maintain presence", func() {
				Expect(cellPresence.CellId).To(Equal(cellID))
				expectedRootFSProviders := models.RootFSProviders{
					"docker":    &models.Providers{ProvidersList: nil},
					"preloaded": &models.Providers{ProvidersList: []string{"the-rootfs"}},
				}
				Expect(cellPresence.RootfsProviders).To(Equal(expectedRootFSProviders))
			})

			It("should have no session health checks", func() {
				sessions, _, err := consulRunner.NewClient().Session().List(nil)
				Expect(err).NotTo(HaveOccurred())

				var repSessions []*api.SessionEntry
				for _, sess := range sessions {
					if sess.Name == "rep" {
						repSessions = append(repSessions, sess)
					}
				}
				Expect(repSessions).To(HaveLen(1))
				Expect(repSessions[0].Checks).To(BeEmpty())
			})

			Context("when the presence fails to be maintained", func() {
				It("should not exit, but keep trying to maintain presence at the same ID", func() {
					consulRunner.Reset()
					// Resetting consul will cause the BBS to die.  But now we get Cells from
					// the BBS we need to restart this process as well after the consul reset
					bbsRunner = bbstestrunner.New(bbsBinPath, bbsArgs)
					bbsProcess = ginkgomon.Invoke(bbsRunner)

					Eventually(fetchCells(logger), 5).Should(HaveLen(1))
					cells, err := bbsClient.Cells()
					cellSet := models.NewCellSetFromList(cells)
					Expect(err).NotTo(HaveOccurred())
					Expect(cellSet[cellID]).To(Equal(cellPresence))

					Expect(runner.Session).NotTo(Exit())
				})
			})
		})

		Context("acting as an auction representative", func() {
			var client rep.Client

			JustBeforeEach(func() {
				Eventually(fetchCells(logger)).Should(HaveLen(1))
				cells, err := bbsClient.Cells()
				cellSet := models.NewCellSetFromList(cells)
				Expect(err).NotTo(HaveOccurred())

				client = rep.NewClient(http.DefaultClient, cf_http.NewCustomTimeoutClient(100*time.Millisecond), cellSet[cellID].RepAddress)
			})

			Context("Capacity with a container", func() {
				It("returns total capacity", func() {
					state, err := client.State()
					Expect(err).NotTo(HaveOccurred())
					Expect(state.TotalResources).To(Equal(rep.Resources{
						MemoryMB:   1024,
						DiskMB:     2048,
						Containers: 4,
					}))
				})

				Context("when the container is removed", func() {
					It("returns available capacity == total capacity", func() {
						fakeGarden.RouteToHandler("GET", "/containers", ghttp.RespondWithJSONEncoded(http.StatusOK, struct{}{}))
						fakeGarden.RouteToHandler("GET", "/containers/bulk_info", ghttp.RespondWithJSONEncoded(http.StatusOK, struct{}{}))

						Eventually(func() rep.Resources {
							state, err := client.State()
							Expect(err).NotTo(HaveOccurred())
							return state.AvailableResources
						}).Should(Equal(rep.Resources{
							MemoryMB:   1024,
							DiskMB:     2048,
							Containers: 4,
						}))
					})
				})
			})
		})

		Describe("polling the BBS for tasks to reap", func() {
			var task *models.Task

			JustBeforeEach(func() {
				task = model_helpers.NewValidTask("task-guid")
				err := bbsClient.DesireTask(task.TaskGuid, task.Domain, task.TaskDefinition)
				Expect(err).NotTo(HaveOccurred())

				_, err = bbsClient.StartTask(task.TaskGuid, cellID)
				Expect(err).NotTo(HaveOccurred())
			})

			It("eventually marks tasks with no corresponding container as failed", func() {
				Eventually(func() []*models.Task {
					return getTasksByState(bbsClient, models.Task_Completed)
				}, 5*pollingInterval).Should(HaveLen(1))

				completedTasks := getTasksByState(bbsClient, models.Task_Completed)

				Expect(completedTasks[0].TaskGuid).To(Equal(task.TaskGuid))
				Expect(completedTasks[0].Failed).To(BeTrue())
			})
		})

		Describe("polling the BBS for actual LRPs to reap", func() {
			JustBeforeEach(func() {
				desiredLRP := &models.DesiredLRP{
					ProcessGuid: "process-guid",
					RootFs:      "some:rootfs",
					Domain:      "some-domain",
					Instances:   1,
					Action: models.WrapAction(&models.RunAction{
						User: "me",
						Path: "the-path",
						Args: []string{},
					}),
				}
				index := 0

				err := bbsClient.DesireLRP(desiredLRP)
				Expect(err).NotTo(HaveOccurred())

				instanceKey := models.NewActualLRPInstanceKey("some-instance-guid", cellID)
				err = bbsClient.ClaimActualLRP(desiredLRP.ProcessGuid, index, &instanceKey)
				Expect(err).NotTo(HaveOccurred())
			})

			It("eventually reaps actual LRPs with no corresponding container", func() {
				Eventually(getActualLRPGroups, 5*pollingInterval).Should(BeEmpty())
			})
		})

		Describe("Evacuation", func() {
			JustBeforeEach(func() {
				resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/evacuate", serverPort), "text/html", nil)
				Expect(err).NotTo(HaveOccurred())
				resp.Body.Close()
				Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
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

		Describe("when a Ping request comes in", func() {
			It("responds with 200 OK", func() {
				resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/ping", serverPort))
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(http.StatusOK))
			})
		})
	})

	Context("when Garden is unavailable", func() {
		BeforeEach(func() {
			runner.StartCheck = ""
		})

		It("should not exit and continue waiting for a connection", func() {
			Consistently(runner.Session.Buffer()).ShouldNot(gbytes.Say("started"))
			Consistently(runner.Session).ShouldNot(Exit())
		})

		Context("when Garden starts", func() {
			JustBeforeEach(func() {
				fakeGarden.Start()
				// these tests only look for the start of a sequence of requests
				fakeGarden.AllowUnhandledRequests = false
			})

			It("should connect", func() {
				Eventually(runner.Session.Buffer(), 5*time.Second).Should(gbytes.Say("started"))
			})
		})
	})
})

func getTasksByState(client bbs.Client, state models.Task_State) []*models.Task {
	tasks, err := client.Tasks()
	Expect(err).NotTo(HaveOccurred())

	filteredTasks := make([]*models.Task, 0)
	for _, task := range tasks {
		if task.State == state {
			filteredTasks = append(filteredTasks, task)
		}
	}
	return filteredTasks
}

func fetchCells(logger lager.Logger) func() ([]*models.CellPresence, error) {
	return func() ([]*models.CellPresence, error) {
		return bbsClient.Cells()
	}
}
