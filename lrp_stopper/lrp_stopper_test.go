package lrp_stopper_test

import (
	"errors"
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/executor/client/fake_client"
	. "github.com/cloudfoundry-incubator/rep/lrp_stopper"
	steno "github.com/cloudfoundry/gosteno"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

var _ = Describe("LRP Stopper", func() {
	var (
		stopper      *LRPStopper
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

		stopper = New(bbs, client, logger)

		process = ifrit.Envoke(stopper)
	})

	AfterEach(func(done Done) {
		process.Signal(os.Interrupt)
		<-process.Wait()
		close(done)
	})

	Context("when waiting for a StopLRPInstance fails", func() {

		var getContainerTimeChan chan time.Time
		var errorTime time.Time

		BeforeEach(func() {
			getContainerTimeChan = make(chan time.Time)

			client.WhenGettingContainer = func(allocationGuid string) (api.Container, error) {
				getContainerTimeChan <- time.Now()
				return api.Container{}, errors.New("oops")
			}

			errorTime = time.Now()
			bbs.WatchForStopLRPInstanceError(errors.New("failed to watch for LRPStopInstance."))
			bbs.EmitStopLRPInstance(stopInstance)
		})

		It("should wait for 3 seconds and retry", func() {
			var getContainerTime time.Time
			Eventually(getContainerTimeChan).Should(Receive(&getContainerTime))
			Ω(getContainerTime.Sub(errorTime)).Should(BeNumerically("~", 3*time.Second, 200*time.Millisecond))
		})

	})

	Context("when a StopLRPInstance request comes down the pipe", func() {
		var didDelete chan struct{}
		var getError error

		BeforeEach(func() {
			getError = nil
		})

		JustBeforeEach(func(done Done) {
			client.WhenGettingContainer = func(allocationGuid string) (api.Container, error) {
				Ω(allocationGuid).Should(Equal(stopInstance.InstanceGuid))
				return api.Container{Guid: stopInstance.InstanceGuid}, getError
			}

			localDidDelete := make(chan struct{})
			didDelete = localDidDelete
			client.WhenDeletingContainer = func(allocationGuid string) error {
				Ω(allocationGuid).Should(Equal(stopInstance.InstanceGuid))
				close(localDidDelete)
				return nil
			}

			bbs.EmitStopLRPInstance(stopInstance)
			close(done)
		})

		Context("when the instance is one that is running on the executor", func() {
			It("should stop the instance", func() {
				Eventually(didDelete).Should(BeClosed())
			})

			It("should resolve the StopLRPInstance", func() {
				Eventually(bbs.ResolvedStopLRPInstances).Should(ContainElement(stopInstance))
			})

			Context("when resolving the container fails", func() {
				BeforeEach(func() {
					bbs.SetResolveStopLRPInstanceError(errors.New("oops"))
				})

				It("should not delete the container", func() {
					Consistently(didDelete).ShouldNot(BeClosed())
				})
			})
		})

		Context("when the instance is not running on the executor", func() {
			BeforeEach(func() {
				getError = errors.New("nope")
			})

			It("should do nothing", func() {
				Consistently(didDelete).ShouldNot(BeClosed())
				Consistently(bbs.ResolvedStopLRPInstances).Should(BeEmpty())
			})
		})
	})
})
