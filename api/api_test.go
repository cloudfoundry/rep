package api_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"

	executorAPI "github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/executor/client/fake_client"
	"github.com/cloudfoundry-incubator/rep/api"
	"github.com/cloudfoundry-incubator/rep/api/lrprunning"
	"github.com/cloudfoundry-incubator/rep/api/taskcomplete"
	"github.com/cloudfoundry-incubator/rep/routes"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gosteno"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/tedsuo/router"
)

var _ = Describe("Callback API", func() {
	var fakeBBS *fake_bbs.FakeRepBBS
	var fakeExecutor *fake_client.FakeClient
	var logger *gosteno.Logger

	var server *httptest.Server
	var httpClient *http.Client

	BeforeSuite(func() {
		gosteno.EnterTestMode(gosteno.LOG_DEBUG)
		logger = gosteno.NewLogger("test-logger")

		httpClient = &http.Client{
			Transport: &http.Transport{},
		}
	})

	BeforeEach(func() {
		fakeBBS = fake_bbs.NewFakeRepBBS()
		fakeExecutor = fake_client.New()

		apiServer, err := api.NewServer(
			taskcomplete.NewHandler(fakeBBS, logger),
			lrprunning.NewHandler(fakeBBS, fakeExecutor, "1.2.3.4", logger),
		)
		Ω(err).ShouldNot(HaveOccurred())

		server = httptest.NewServer(apiServer)
	})

	AfterEach(func() {
		server.Close()
	})

	Describe("PUT /task_completed/:guid", func() {
		var task models.Task
		var result executorAPI.ContainerRunResult

		var resp *http.Response

		BeforeEach(func() {
			index := 0

			task = models.Task{
				Guid:       "task-guid-123",
				Stack:      "some-stack",
				MemoryMB:   64,
				DiskMB:     1024,
				CpuPercent: .5,

				ExecutorID:      "some-executor-id",
				ContainerHandle: "some-container-handle",

				Actions: []models.ExecutorAction{
					{
						Action: models.RunAction{
							Script:  "the-script",
							Env:     []models.EnvironmentVariable{{Key: "PATH", Value: "the-path"}},
							Timeout: 500,
						},
					},
				},

				Log: models.LogConfig{
					Guid:       "some-guid",
					SourceName: "XYZ",
					Index:      &index,
				},
			}

			marshalledTask, err := json.Marshal(task)
			Ω(err).ShouldNot(HaveOccurred())

			result = executorAPI.ContainerRunResult{
				Metadata: marshalledTask,
			}
		})

		JustBeforeEach(func() {
			body, err := json.Marshal(result)
			Ω(err).ShouldNot(HaveOccurred())

			request, err := http.NewRequest("PUT", server.URL+"/task_completed/"+task.Guid, bytes.NewReader(body))
			Ω(err).ShouldNot(HaveOccurred())

			resp, err = httpClient.Do(request)
			Ω(err).ShouldNot(HaveOccurred())
		})

		Describe("when the task succeeds", func() {
			BeforeEach(func() {
				result.Result = "42"
			})

			It("responds to the onComplete hook", func() {
				Ω(resp.StatusCode).Should(Equal(http.StatusOK))
			})

			It("records the job result", func() {
				Eventually(fakeBBS.CompletedTasks).Should(HaveLen(1))
				Ω(fakeBBS.CompletedTasks()[0].Guid).Should(Equal(task.Guid))
				Ω(fakeBBS.CompletedTasks()[0].Result).Should(Equal("42"))
			})
		})

		Describe("when the task fails", func() {
			BeforeEach(func() {
				result.Failed = true
				result.FailureReason = "it didn't work"
			})

			It("responds to the onComplete hook", func() {
				Ω(resp.StatusCode).Should(Equal(http.StatusOK))
			})

			It("records the job failure", func() {
				expectedTask := task
				expectedTask.ContainerHandle = "some-container-handle"
				expectedTask.ExecutorID = "some-executor-id"
				expectedTask.Failed = true
				expectedTask.FailureReason = "it didn't work"

				Eventually(fakeBBS.CompletedTasks).Should(HaveLen(1))
				Ω(fakeBBS.CompletedTasks()[0]).Should(Equal(expectedTask))
			})
		})
	})

	Describe("PUT /lrp_running/:process_guid/:index/:instance_guid", func() {
		var processGuid string
		var index string
		var instanceGuid string

		var response *http.Response

		BeforeEach(func() {
			processGuid = "some-process-guid"
			index = "2"
			instanceGuid = "some-instance-guid"
		})

		JustBeforeEach(func() {
			generator := router.NewRequestGenerator(server.URL, routes.Routes)

			request, err := generator.RequestForHandler(routes.LRPRunning, router.Params{
				"process_guid":  processGuid,
				"index":         index,
				"instance_guid": instanceGuid,
			}, nil)
			Ω(err).ShouldNot(HaveOccurred())

			response, err = httpClient.Do(request)
			Ω(err).ShouldNot(HaveOccurred())
		})

		Context("when the guid is found on the executor", func() {
			BeforeEach(func() {
				fakeExecutor.WhenGettingContainer = func(guid string) (executorAPI.Container, error) {
					return executorAPI.Container{
						Ports: []executorAPI.PortMapping{
							{ContainerPort: 8080, HostPort: 1234},
							{ContainerPort: 8081, HostPort: 1235},
						},
					}, nil
				}
			})

			It("reports the LRP as running", func() {
				Ω(fakeBBS.RunningLRPs()).Should(Equal([]models.ActualLRP{
					{
						ProcessGuid:  "some-process-guid",
						Index:        2,
						InstanceGuid: "some-instance-guid",

						Host: "1.2.3.4",

						Ports: []models.PortMapping{
							{ContainerPort: 8080, HostPort: 1234},
							{ContainerPort: 8081, HostPort: 1235},
						},
					},
				}))
			})

			It("returns 200", func() {
				Ω(response.StatusCode).Should(Equal(http.StatusOK))
			})
		})

		Context("when the guid is not found on the executor", func() {
			disaster := errors.New("oh no!")

			BeforeEach(func() {
				fakeExecutor.WhenGettingContainer = func(guid string) (executorAPI.Container, error) {
					return executorAPI.Container{}, disaster
				}
			})

			It("returns 400", func() {
				Ω(response.StatusCode).Should(Equal(http.StatusBadRequest))
			})
		})

		Context("when the index is not a number", func() {
			BeforeEach(func() {
				index = "nope"
			})

			It("returns 400", func() {
				Ω(response.StatusCode).Should(Equal(http.StatusBadRequest))
			})
		})

		Context("when reporting it as running fails", func() {
			BeforeEach(func() {
				fakeBBS.SetRunningError(errors.New("oh no!"))
			})

			It("returns 500", func() {
				Ω(response.StatusCode).Should(Equal(http.StatusInternalServerError))
			})
		})
	})
})
