package integration_test

import (
	"fmt"
	"net/http"
	"time"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/auction/communication/nats/auction_nats_client"
	"github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/rep/reprunner"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/gunk/timeprovider"

	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/onsi/gomega/ghttp"
)

var runner *reprunner.Runner

var _ = Describe("The Rep", func() {
	var (
		fakeExecutor *ghttp.Server
		bbs          *Bbs.BBS
	)

	BeforeEach(func() {
		fakeExecutor = ghttp.NewServer()
		// these tests only look for the start of a sequence of requests
		fakeExecutor.AllowUnhandledRequests = true
		fakeExecutor.AppendHandlers(ghttp.CombineHandlers(
			ghttp.VerifyRequest("GET", "/ping"),
			ghttp.RespondWith(http.StatusOK, nil),
		))

		bbs = Bbs.NewBBS(etcdRunner.Adapter(), timeprovider.NewTimeProvider(), gosteno.NewLogger("the-logger"))
		runner = reprunner.New(
			representativePath,
			"the-stack",
			"the-lrp-host",
			fmt.Sprintf("127.0.0.1:%d", schedulerPort),
			fakeExecutor.URL(),
			fmt.Sprintf("http://127.0.0.1:%d", etcdPort),
			"127.0.0.1:4001",
			"info",
			5*time.Second,
		)

		runner.Start()
	})

	AfterEach(func(done Done) {
		runner.KillWithFire()
		fakeExecutor.Close()
		close(done)
	})

	Describe("when an interrupt signal is send to the representative", func() {
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
			Ω(executorPresence.ExecutorID).ShouldNot(BeZero())
		})

		Context("when the presence fails to be maintained", func() {
			It("should not exit, but keep trying to maintain presence at the same ID", func() {
				fakeExecutor.AppendHandlers(ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/ping"),
					ghttp.RespondWith(http.StatusOK, nil),
				))

				etcdRunner.Stop()
				etcdRunner.Start()

				Eventually(bbs.GetAllExecutors, 10).Should(HaveLen(1))
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
				Guid:  "the-task-guid",
				Stack: "the-stack",
				Actions: []models.ExecutorAction{
					{
						Action: models.RunAction{
							Script: "cat /tmp/file",
						},
					},
				},
			})
		})

		It("makes a request to the executor", func() {
			Eventually(fakeExecutor.ReceivedRequests).Should(HaveLen(1))
		})
	})

	Describe("acting as an auction representative", func() {
		BeforeEach(func() {
			fakeExecutor.AppendHandlers(ghttp.CombineHandlers(
				ghttp.VerifyRequest("GET", "/resources/total"),
				ghttp.RespondWithJSONEncoded(http.StatusOK, api.ExecutorResources{
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

			client, err := auction_nats_client.New(natsRunner.MessageBus, time.Second, 10*time.Second, gosteno.NewLogger("the-logger"))
			Ω(err).ShouldNot(HaveOccurred())
			resources := client.TotalResources(executorID)
			Ω(resources).Should(Equal(auctiontypes.Resources{
				MemoryMB:   1024,
				DiskMB:     2048,
				Containers: 4,
			}))
		})
	})

	Describe("when a StopLRPInstance request comes in", func() {
		BeforeEach(func() {
			fakeExecutor.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/containers/some-process-guid.3.some-instance-guid"),
					ghttp.RespondWithJSONEncoded(http.StatusOK, api.Container{
						Guid: "some-instance-guid",
					}),
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("DELETE", "/containers/some-process-guid.3.some-instance-guid"),
					ghttp.RespondWith(http.StatusOK, nil),
				),
			)

			bbs.ReportActualLRPAsRunning(models.ActualLRP{
				ProcessGuid:  "some-process-guid",
				InstanceGuid: "some-instance-guid",
				Index:        3,
			}, "executor-guid")

			bbs.RequestStopLRPInstance(models.StopLRPInstance{
				ProcessGuid:  "some-process-guid",
				InstanceGuid: "some-instance-guid",
				Index:        3,
			})
		})

		It("should delete the container and resolve the StopLRPInstance", func() {
			Eventually(fakeExecutor.ReceivedRequests).Should(HaveLen(3))
			Eventually(bbs.GetAllActualLRPs).Should(BeEmpty())
			Eventually(bbs.GetAllStopLRPInstances).Should(BeEmpty())
		})
	})
})
