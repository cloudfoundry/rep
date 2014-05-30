package integration_test

import (
	"fmt"
	"net/http"
	"time"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/auction/communication/nats/repnatsclient"
	"github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/rep/reprunner"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gunk/timeprovider"

	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	steno "github.com/cloudfoundry/gosteno"
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

		bbs = Bbs.NewBBS(etcdRunner.Adapter(), timeprovider.NewTimeProvider(), steno.NewLogger("the-logger"))

		runner = reprunner.New(
			representativePath,
			"the-stack",
			"the-lrp-host",
			fmt.Sprintf("127.0.0.1:%d", schedulerPort),
			fakeExecutor.URL(),
			fmt.Sprintf("http://127.0.0.1:%d", etcdPort),
			"127.0.0.1:4001",
			"info",
			time.Second,
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
			Eventually(runner.LastExitCode).Should(Equal(0))
		})
	})

	Describe("maintaining presence", func() {
		var repPresence models.RepPresence

		BeforeEach(func() {
			Eventually(bbs.GetAllReps).Should(HaveLen(1))
			reps, err := bbs.GetAllReps()
			Ω(err).ShouldNot(HaveOccurred())
			repPresence = reps[0]
		})

		It("should maintain presence", func() {
			Ω(repPresence.Stack).Should(Equal("the-stack"))
			Ω(repPresence.RepID).ShouldNot(BeZero())
		})

		Context("when the presence fails to be maintained", func() {
			It("should not exit, but keep trying to maintain presence at the same ID", func() {
				etcdRunner.Stop()
				etcdRunner.Start()

				Eventually(bbs.GetAllReps).Should(HaveLen(1))
				reps, err := bbs.GetAllReps()
				Ω(err).ShouldNot(HaveOccurred())
				Ω(reps[0]).Should(Equal(repPresence))

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
			Eventually(bbs.GetAllReps).Should(HaveLen(1))
			reps, err := bbs.GetAllReps()
			Ω(err).ShouldNot(HaveOccurred())
			repID := reps[0].RepID

			client := repnatsclient.New(natsRunner.MessageBus, time.Second, 10*time.Second)
			resources := client.TotalResources(repID)
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
					ghttp.VerifyRequest("GET", "/containers/some-instance-guid"),
					ghttp.RespondWithJSONEncoded(http.StatusOK, api.Container{
						Guid: "some-instance-guid",
					}),
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("DELETE", "/containers/some-instance-guid"),
					ghttp.RespondWith(http.StatusOK, nil),
				),
			)

			bbs.ReportActualLRPAsRunning(models.ActualLRP{
				ProcessGuid:  "some-process-guid",
				InstanceGuid: "some-instance-guid",
				Index:        3,
			})

			bbs.RequestStopLRPInstance(models.StopLRPInstance{
				ProcessGuid:  "some-process-guid",
				InstanceGuid: "some-instance-guid",
				Index:        3,
			})
		})

		It("should delete the container and resolve the StopLRPInstance", func() {
			Eventually(fakeExecutor.ReceivedRequests).Should(HaveLen(2))
			Eventually(bbs.GetAllActualLRPs).Should(BeEmpty())
			Eventually(bbs.GetAllStopLRPInstances).Should(BeEmpty())
		})
	})
})
