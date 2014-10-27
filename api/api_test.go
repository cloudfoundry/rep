package api_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"

	"github.com/cloudfoundry-incubator/executor"
	fake_client "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep/api"
	"github.com/cloudfoundry-incubator/rep/api/lrprunning"
	"github.com/cloudfoundry-incubator/rep/api/taskcomplete"
	"github.com/cloudfoundry-incubator/rep/routes"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/pivotal-golang/archiver/extractor/test_helper"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/rata"
)

var _ = Describe("Callback API", func() {
	var fakeBBS *fake_bbs.FakeRepBBS
	var fakeExecutor *fake_client.FakeClient
	var logger lager.Logger

	var server *httptest.Server
	var httpClient *http.Client

	BeforeSuite(func() {
		logger = lagertest.NewTestLogger("test")

		httpClient = &http.Client{
			Transport: &http.Transport{},
		}
	})

	BeforeEach(func() {
		fakeBBS = &fake_bbs.FakeRepBBS{}
		fakeExecutor = new(fake_client.FakeClient)

		apiServer, err := api.NewServer(
			taskcomplete.NewHandler(fakeBBS, fakeExecutor, logger),
			lrprunning.NewHandler("some-executor-id", fakeBBS, fakeExecutor, "1.2.3.4", logger),
		)
		Ω(err).ShouldNot(HaveOccurred())

		server = httptest.NewServer(apiServer)
	})

	AfterEach(func() {
		server.Close()
	})

	Describe("PUT /task_completed/:guid", func() {
		var task models.Task
		var result executor.ContainerRunResult

		var resp *http.Response

		BeforeEach(func() {
			task = models.Task{
				TaskGuid:  "task-guid-123",
				Stack:     "some-stack",
				MemoryMB:  64,
				DiskMB:    1024,
				CPUWeight: 5,

				ExecutorID:      "some-executor-id",
				ContainerHandle: "some-container-handle",
				ResultFile:      "some-file",

				Actions: []models.ExecutorAction{
					{
						Action: models.RunAction{
							Path:    "the-script",
							Env:     []models.EnvironmentVariable{{Name: "PATH", Value: "the-path"}},
							Timeout: 500,
						},
					},
				},

				Log: models.LogConfig{
					Guid:       "some-guid",
					SourceName: "XYZ",
				},
			}

			result = executor.ContainerRunResult{
				Guid: "task-guid-123",
			}
		})

		JustBeforeEach(func() {
			body, err := json.Marshal(result)
			Ω(err).ShouldNot(HaveOccurred())

			request, err := http.NewRequest("PUT", server.URL+"/task_completed/"+task.TaskGuid, bytes.NewReader(body))
			Ω(err).ShouldNot(HaveOccurred())

			resp, err = httpClient.Do(request)
			Ω(err).ShouldNot(HaveOccurred())
		})

		Context("when the task succeeds", func() {
			BeforeEach(func() {
				dest := gbytes.NewBuffer()
				test_helper.WriteTar(
					dest,
					[]test_helper.ArchiveFile{{
						Name: "some-file",
						Body: "42",
						Mode: 0600,
						Dir:  false,
					}},
				)
				fakeExecutor.GetFilesReturns(dest, nil)

				fakeBBS.GetTaskByGuidReturns(task, nil)
			})

			It("responds to the onComplete hook", func() {
				Ω(resp.StatusCode).Should(Equal(http.StatusOK))
			})

			It("records the job result", func() {
				Eventually(fakeBBS.CompleteTaskCallCount).Should(Equal(1))
				taskGuid, failed, failureReason, result := fakeBBS.CompleteTaskArgsForCall(0)
				Ω(taskGuid).Should(Equal(task.TaskGuid))
				Ω(failed).Should(BeFalse())
				Ω(failureReason).Should(BeEmpty())
				Ω(result).Should(Equal("42"))
			})

			It("deletes the container", func() {
				Ω(fakeExecutor.DeleteContainerCallCount()).Should(Equal(1))
				Ω(fakeExecutor.DeleteContainerArgsForCall(0)).Should(Equal(task.TaskGuid))
			})
		})

		Context("when the task succeeds but getting the task fails", func() {
			BeforeEach(func() {
				dest := gbytes.NewBuffer()
				test_helper.WriteTar(
					dest,
					[]test_helper.ArchiveFile{{
						Name: "some-file",
						Body: "42",
						Mode: 0600,
						Dir:  false,
					}},
				)
				fakeExecutor.GetFilesReturns(dest, nil)

				fakeBBS.GetTaskByGuidReturns(models.Task{}, errors.New("nope"))
			})

			It("responds to the onComplete hook", func() {
				Ω(resp.StatusCode).Should(Equal(http.StatusOK))
			})

			It("records the job result as a failure", func() {
				Eventually(fakeBBS.CompleteTaskCallCount).Should(Equal(1))
				taskGuid, failed, failureReason, result := fakeBBS.CompleteTaskArgsForCall(0)
				Ω(taskGuid).Should(Equal(task.TaskGuid))
				Ω(failed).Should(BeTrue())
				Ω(failureReason).Should(Equal("failed to fetch task: 'nope'"))
				Ω(result).Should(Equal(""))
			})

			It("deletes the container", func() {
				Ω(fakeExecutor.DeleteContainerCallCount()).Should(Equal(1))
				Ω(fakeExecutor.DeleteContainerArgsForCall(0)).Should(Equal(task.TaskGuid))
			})
		})

		Context("when the task succeeds but getting the result file stream fails", func() {
			BeforeEach(func() {
				fakeExecutor.GetFilesReturns(nil, errors.New("nope"))

				fakeBBS.GetTaskByGuidReturns(task, nil)
			})

			It("responds to the onComplete hook", func() {
				Ω(resp.StatusCode).Should(Equal(http.StatusOK))
			})

			It("records the job result as a failure", func() {
				Eventually(fakeBBS.CompleteTaskCallCount).Should(Equal(1))
				taskGuid, failed, failureReason, result := fakeBBS.CompleteTaskArgsForCall(0)
				Ω(taskGuid).Should(Equal(task.TaskGuid))
				Ω(failed).Should(BeTrue())
				Ω(failureReason).Should(Equal("failed to fetch result: 'nope'"))
				Ω(result).Should(Equal(""))
			})

			It("deletes the container", func() {
				Ω(fakeExecutor.DeleteContainerCallCount()).Should(Equal(1))
				Ω(fakeExecutor.DeleteContainerArgsForCall(0)).Should(Equal(task.TaskGuid))
			})
		})

		Context("when the task succeeds but getting the contents of the result file fails", func() {
			BeforeEach(func() {
				dest := gbytes.NewBuffer()
				test_helper.WriteTar(
					dest,
					[]test_helper.ArchiveFile{},
				)
				fakeExecutor.GetFilesReturns(dest, nil)

				fakeBBS.GetTaskByGuidReturns(task, nil)
			})

			It("responds to the onComplete hook", func() {
				Ω(resp.StatusCode).Should(Equal(http.StatusOK))
			})

			It("records the job result as a failure", func() {
				Eventually(fakeBBS.CompleteTaskCallCount).Should(Equal(1))
				taskGuid, failed, failureReason, result := fakeBBS.CompleteTaskArgsForCall(0)
				Ω(taskGuid).Should(Equal(task.TaskGuid))
				Ω(failed).Should(BeTrue())
				Ω(failureReason).Should(ContainSubstring("failed to read contents of the result file"))
				Ω(result).Should(Equal(""))
			})

			It("deletes the container", func() {
				Ω(fakeExecutor.DeleteContainerCallCount()).Should(Equal(1))
				Ω(fakeExecutor.DeleteContainerArgsForCall(0)).Should(Equal(task.TaskGuid))
			})
		})

		Context("when the task succeeds but the contents of the result file is too big", func() {
			BeforeEach(func() {
				dest := gbytes.NewBuffer()
				test_helper.WriteTar(
					dest,
					[]test_helper.ArchiveFile{{
						Name: "some-file",
						Body: string(make([]byte, 10241)),
						Mode: 0600,
						Dir:  false,
					}},
				)
				fakeExecutor.GetFilesReturns(dest, nil)

				fakeBBS.GetTaskByGuidReturns(task, nil)
			})

			It("responds to the onComplete hook", func() {
				Ω(resp.StatusCode).Should(Equal(http.StatusOK))
			})

			It("records the job result as a failure", func() {
				Eventually(fakeBBS.CompleteTaskCallCount).Should(Equal(1))
				taskGuid, failed, failureReason, result := fakeBBS.CompleteTaskArgsForCall(0)
				Ω(taskGuid).Should(Equal(task.TaskGuid))
				Ω(failed).Should(BeTrue())
				Ω(failureReason).Should(Equal("result file size is 10241, max bytes allowed is 10240"))
				Ω(result).Should(Equal(""))
			})

			It("deletes the container", func() {
				Ω(fakeExecutor.DeleteContainerCallCount()).Should(Equal(1))
				Ω(fakeExecutor.DeleteContainerArgsForCall(0)).Should(Equal(task.TaskGuid))
			})
		})

		Context("when the task fails", func() {
			BeforeEach(func() {
				result.Failed = true
				result.FailureReason = "it didn't work"
			})

			It("responds to the onComplete hook", func() {
				Ω(resp.StatusCode).Should(Equal(http.StatusOK))
			})

			It("Does not attempt to fetch the result file", func() {
				Ω(fakeExecutor.GetFilesCallCount()).Should(BeZero())
			})

			It("records the job failure", func() {
				Eventually(fakeBBS.CompleteTaskCallCount).Should(Equal(1))
				taskGuid, failed, failureReason, result := fakeBBS.CompleteTaskArgsForCall(0)
				Ω(taskGuid).Should(Equal(task.TaskGuid))
				Ω(failed).Should(BeTrue())
				Ω(failureReason).Should(Equal("it didn't work"))
				Ω(result).Should(BeEmpty())
			})

			It("deletes the container", func() {
				Ω(fakeExecutor.DeleteContainerCallCount()).Should(Equal(1))
				Ω(fakeExecutor.DeleteContainerArgsForCall(0)).Should(Equal(task.TaskGuid))
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
			generator := rata.NewRequestGenerator(server.URL, routes.Routes)

			request, err := generator.CreateRequest(routes.LRPRunning, rata.Params{
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
				container := executor.Container{
					Ports: []executor.PortMapping{
						{ContainerPort: 8080, HostPort: 1234},
						{ContainerPort: 8081, HostPort: 1235},
					},
				}
				fakeExecutor.GetContainerReturns(container, nil)
			})

			It("reports the LRP as running", func() {
				actualLRP, executorGUID := fakeBBS.ReportActualLRPAsRunningArgsForCall(0)
				Ω(actualLRP).Should(Equal(models.ActualLRP{
					ProcessGuid:  "some-process-guid",
					Index:        2,
					InstanceGuid: "some-instance-guid",

					Host: "1.2.3.4",

					Ports: []models.PortMapping{
						{ContainerPort: 8080, HostPort: 1234},
						{ContainerPort: 8081, HostPort: 1235},
					},
				}))

				Ω(executorGUID).Should(Equal("some-executor-id"))
			})

			It("returns 200", func() {
				Ω(response.StatusCode).Should(Equal(http.StatusOK))
			})
		})

		Context("when the guid is not found on the executor", func() {
			disaster := errors.New("oh no!")

			BeforeEach(func() {
				fakeExecutor.GetContainerReturns(executor.Container{}, disaster)
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
				fakeBBS.ReportActualLRPAsRunningReturns(errors.New("oh no!"))
			})

			It("returns 500", func() {
				Ω(response.StatusCode).Should(Equal(http.StatusInternalServerError))
			})
		})
	})
})
