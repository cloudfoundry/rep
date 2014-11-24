package stop_lrp_listener_test

import (
	"errors"
	"os"

	"github.com/cloudfoundry-incubator/rep/lrp_stopper/fake_lrp_stopper"
	. "github.com/cloudfoundry-incubator/rep/stop_lrp_listener"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"
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
		logger       lager.Logger
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

		logger = lagertest.NewTestLogger("test")
		stopper = &fake_lrp_stopper.FakeLRPStopper{}

		listener := New(stopper, fakeBBS, logger)

		process = ifrit.Envoke(listener)
	})

	AfterEach(func() {
		process.Signal(os.Interrupt)
		Eventually(process.Wait()).Should(Receive())
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

	Context("when waiting for a StopLRPInstance fails", func() {
		BeforeEach(func() {
			watchErrorChan <- errors.New("failed to watch for LRPStopInstance.")
		})

		It("should retry", func() {
			Eventually(fakeBBS.WatchForStopLRPInstanceCallCount).Should(Equal(2))
		})
	})
})
