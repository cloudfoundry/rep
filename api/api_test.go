package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/cloudfoundry-incubator/executor/client"
	"github.com/cloudfoundry-incubator/executor/client/fake_client"
	"github.com/cloudfoundry-incubator/rep/api"
	"github.com/cloudfoundry-incubator/rep/api/taskcomplete"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gosteno"
)

var _ = Describe("Callback API", func() {
	var fakeBBS *fake_bbs.FakeRepBBS
	var logger *gosteno.Logger

	var server *httptest.Server

	BeforeSuite(func() {
		gosteno.EnterTestMode(gosteno.LOG_DEBUG)
		logger = gosteno.NewLogger("test-logger")
	})

	Describe("POST /task_completed/:guid", func() {
		var task models.Task
		var result client.ContainerRunResult

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

			fakeBBS = fake_bbs.NewFakeRepBBS()

			apiServer, err := api.NewServer(taskcomplete.NewHandler(fakeBBS, logger), nil)
			Ω(err).ShouldNot(HaveOccurred())

			server = httptest.NewServer(apiServer)

			marshalledTask, err := json.Marshal(task)
			Ω(err).ShouldNot(HaveOccurred())

			result = client.ContainerRunResult{
				Metadata: marshalledTask,
			}
		})

		JustBeforeEach(func() {
			body, err := fake_client.MarshalContainerRunResult(result)
			Ω(err).ShouldNot(HaveOccurred())

			resp, err = http.Post(server.URL+"/task_completed/"+task.Guid, "application/json", bytes.NewReader(body))
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
})
