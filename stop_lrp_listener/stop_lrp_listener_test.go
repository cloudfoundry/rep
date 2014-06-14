package stop_lrp_listener_test

import (
	"errors"
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor/client/fake_client"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper/fake_lrp_stopper"
	. "github.com/cloudfoundry-incubator/rep/stop_lrp_listener"
	steno "github.com/cloudfoundry/gosteno"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

var _ = Describe("StopLRPListener", func() {
	var (
		stopper      *fake_lrp_stopper.FakeLRPStopper
		bbs          *fake_bbs.FakeRepBBS
		client       *fake_client.FakeClient
		logger       *steno.Logger
		process      ifrit.Process
		stopInstance models.StopLRPInstance
	)

	BeforeEach(func() {
		stopInstance = models.StopLRPInstance{
			ProcessGuid:  "some-process-guid",
			InstanceGuid: "some-instance-guid",
			Index:        1138,
		}

		steno.EnterTestMode()
		bbs = fake_bbs.NewFakeRepBBS()
		client = fake_client.New()
		logger = steno.NewLogger("steno")
		stopper = &fake_lrp_stopper.FakeLRPStopper{}

		listener := New(stopper, bbs, client, logger)

		process = ifrit.Envoke(listener)
	})

	AfterEach(func() {
		process.Signal(os.Interrupt)
		Eventually(process.Wait()).Should(Receive())
	})

	Context("when waiting for a StopLRPInstance fails", func() {
		var getContainerTimeChan chan time.Time
		var errorTime time.Time

		BeforeEach(func() {
			getContainerTimeChan = make(chan time.Time)

			stopper.StopInstanceStub = func(stopInstance models.StopLRPInstance) error {
				getContainerTimeChan <- time.Now()
				return nil
			}

			errorTime = time.Now()
			bbs.WatchForStopLRPInstanceError(errors.New("failed to watch for LRPStopInstance."))
		})

		Context("and a stop event comes in", func() {
			BeforeEach(func() {
				bbs.EmitStopLRPInstance(stopInstance)
			})

			It("should wait for 3 seconds and retry", func() {
				var getContainerTime time.Time
				Eventually(getContainerTimeChan).Should(Receive(&getContainerTime))
				Ω(getContainerTime.Sub(errorTime)).Should(BeNumerically("~", 3*time.Second, 200*time.Millisecond))
			})
		})

		Context("and SIGINT is received", func() {
			BeforeEach(func() {
				process.Signal(os.Interrupt)
			})

			It("should exit quickly", func() {
				Eventually(process.Wait()).Should(Receive())
			})
		})
	})

	Context("when a StopLRPInstance request comes down the pipe", func() {
		JustBeforeEach(func() {
			bbs.EmitStopLRPInstance(stopInstance)
		})

		It("should inform the stopper", func() {
			Eventually(stopper.StopInstanceCallCount).Should(Equal(1))
			Ω(stopper.StopInstanceArgsForCall(0)).Should(Equal(stopInstance))
		})
	})
})
