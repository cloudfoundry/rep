package lrp_stopper_test

import (
	"errors"
	"os"

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

	Context("when a StopLRPInstance request comes down the pipe", func() {
		var didDelete chan struct{}
		var getError, deleteError error
		JustBeforeEach(func(done Done) {
			client.WhenGettingContainer = func(allocationGuid string) (api.Container, error) {
				Ω(allocationGuid).Should(Equal(stopInstance.InstanceGuid))
				return api.Container{Guid: stopInstance.InstanceGuid}, getError
			}

			didDelete = make(chan struct{})
			client.WhenDeletingContainer = func(allocationGuid string) error {
				Ω(allocationGuid).Should(Equal(stopInstance.InstanceGuid))
				close(didDelete)
				return deleteError
			}

			bbs.EmitStopLRPInstance(stopInstance)
			close(done)
		})

		BeforeEach(func() {
			getError, deleteError = nil, nil
		})

		Context("when the instance is one that is running on the executor", func() {
			It("should stop the instance", func() {
				Eventually(didDelete).Should(BeClosed())
			})

			It("should resolve the StopLRPInstance", func() {
				Eventually(bbs.ResolvedStopLRPInstances).Should(ContainElement(stopInstance))
			})

			Context("when deleting the container fails", func() {
				BeforeEach(func() {
					deleteError = errors.New("oops")
				})

				It("should not resolve the StopLRPInstance", func() {
					Consistently(bbs.ResolvedStopLRPInstances).Should(BeEmpty())
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
