package stop_lrp_listener_test

import (
	"errors"
	"os"
	"time"

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
		fakeBBS      *fake_bbs.FakeRepBBS
		logger       *steno.Logger
		process      ifrit.Process
		stopInstance models.StopLRPInstance

		stopInstanceChan chan models.StopLRPInstance
		watchStopChan    chan bool
		watchErrorChan   chan error
	)

	BeforeEach(func() {
		stopInstance = models.StopLRPInstance{
			ProcessGuid:  "some-process-guid",
			InstanceGuid: "some-instance-guid",
			Index:        1138,
		}

		fakeBBS = &fake_bbs.FakeRepBBS{}
		stopInstanceChan = make(chan models.StopLRPInstance, 0)
		watchStopChan = make(chan bool, 0)
		watchErrorChan = make(chan error, 0)

		fakeBBS.WatchForStopLRPInstanceReturns(stopInstanceChan, watchStopChan, watchErrorChan)

		steno.EnterTestMode()
		logger = steno.NewLogger("steno")
		stopper = &fake_lrp_stopper.FakeLRPStopper{}

		listener := New(stopper, fakeBBS, logger)

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
			watchErrorChan <- errors.New("failed to watch for LRPStopInstance.")
		})

		Context("and a stop event comes in", func() {
			BeforeEach(func() {
				stopInstanceChan <- stopInstance
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
			stopInstanceChan <- stopInstance
		})

		It("should inform the stopper", func() {
			Eventually(stopper.StopInstanceCallCount).Should(Equal(1))
			Ω(stopper.StopInstanceArgsForCall(0)).Should(Equal(stopInstance))
		})
	})
})
