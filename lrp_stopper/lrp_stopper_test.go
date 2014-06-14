package lrp_stopper_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/executor/client/fake_client"
	. "github.com/cloudfoundry-incubator/rep/lrp_stopper"
	steno "github.com/cloudfoundry/gosteno"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

var _ = Describe("LRP Stopper", func() {
	var (
		stopper      LRPStopper
		bbs          *fake_bbs.FakeRepBBS
		client       *fake_client.FakeClient
		logger       *steno.Logger
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
	})

	Context("when told to stop an instance", func() {
		var didDelete bool
		var getError error
		var returnedError error

		BeforeEach(func() {
			didDelete = false
			getError = nil
			returnedError = nil
		})

		JustBeforeEach(func() {
			client.WhenGettingContainer = func(allocationGuid string) (api.Container, error) {
				Ω(allocationGuid).Should(Equal(stopInstance.InstanceGuid))
				err := getError
				return api.Container{Guid: stopInstance.InstanceGuid}, err
			}

			client.WhenDeletingContainer = func(allocationGuid string) error {
				Ω(allocationGuid).Should(Equal(stopInstance.InstanceGuid))
				didDelete = true
				return nil
			}

			returnedError = stopper.StopInstance(stopInstance)
		})

		Context("when the instance is one that is running on the executor", func() {
			It("should stop the instance", func() {
				Ω(didDelete).Should(BeTrue())
			})

			It("should resolve the StopLRPInstance", func() {
				Ω(bbs.ResolvedStopLRPInstances()).Should(ContainElement(stopInstance))
			})

			It("should not error", func() {
				Ω(returnedError).ShouldNot(HaveOccurred())
			})

			Context("when resolving the container fails", func() {
				BeforeEach(func() {
					bbs.SetResolveStopLRPInstanceError(errors.New("oops"))
				})

				It("should not delete the container", func() {
					Ω(didDelete).Should(BeFalse())
				})

				It("should return an error error", func() {
					Ω(returnedError).Should(MatchError(errors.New("oops")))
				})
			})
		})

		Context("when the instance is not running on the executor", func() {
			BeforeEach(func() {
				getError = errors.New("nope")
			})

			It("should do nothing", func() {
				Ω(didDelete).Should(BeFalse())
				Ω(bbs.ResolvedStopLRPInstances()).Should(BeEmpty())
			})

			It("should return an error error", func() {
				Ω(returnedError).Should(MatchError(errors.New("nope")))
			})
		})
	})
})
