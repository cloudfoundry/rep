package lrp_stopper_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/executor"
	fake_client "github.com/cloudfoundry-incubator/executor/fakes"
	. "github.com/cloudfoundry-incubator/rep/lrp_stopper"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

var _ = Describe("LRP Stopper", func() {
	var (
		cellID    string = "the-cell-id"
		stopper   LRPStopper
		bbs       *fake_bbs.FakeRepBBS
		client    *fake_client.FakeClient
		logger    lager.Logger
		actualLRP models.ActualLRP
	)

	BeforeEach(func() {
		actualLRP = models.ActualLRP{
			ActualLRPKey: models.NewActualLRPKey(
				"some-process-guid",
				1138,
				"some-domain",
			),
			ActualLRPContainerKey: models.NewActualLRPContainerKey(
				"some-instance-guid",
				"some-cell-id",
			),
		}

		bbs = &fake_bbs.FakeRepBBS{}
		client = new(fake_client.FakeClient)
		logger = lagertest.NewTestLogger("test")

		stopper = New(cellID, bbs, client, logger)
	})

	Context("when told to stop an instance", func() {
		var returnedError error

		JustBeforeEach(func() {
			returnedError = stopper.StopInstance(actualLRP)
		})

		It("attempts to delete the container", func() {
			Ω(client.DeleteContainerCallCount()).Should(Equal(1))

			allocationGuid := client.DeleteContainerArgsForCall(0)
			Ω(allocationGuid).Should(Equal(actualLRP.InstanceGuid))
		})

		It("marks the LRP as stopped", func() {
			lrpKey, containerKey := bbs.RemoveActualLRPArgsForCall(0)
			Ω(lrpKey).Should(Equal(actualLRP.ActualLRPKey))
			Ω(containerKey).Should(Equal(actualLRP.ActualLRPContainerKey))
		})

		It("succeeds", func() {
			Ω(returnedError).ShouldNot(HaveOccurred())
		})

		Context("when deleting the container fails", func() {
			BeforeEach(func() {
				client.DeleteContainerReturns(errors.New("Couldn't delete that"))
			})

			It("does not mark the LRP as stopped", func() {
				Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(0))
			})
		})

		Context("when the executor returns 'not found'", func() {
			BeforeEach(func() {
				client.DeleteContainerReturns(executor.ErrContainerNotFound)
			})

			It("marks the LRP as stopped", func() {
				lrpKey, containerKey := bbs.RemoveActualLRPArgsForCall(0)
				Ω(lrpKey).Should(Equal(actualLRP.ActualLRPKey))
				Ω(containerKey).Should(Equal(actualLRP.ActualLRPContainerKey))
			})

			It("succeeds because the container is apparently already gone", func() {
				Ω(returnedError).ShouldNot(HaveOccurred())
			})
		})

		Context("when marking the LRP as stopped fails", func() {
			var expectedError = errors.New("ker-blewie")

			BeforeEach(func() {
				bbs.RemoveActualLRPReturns(expectedError)
			})

			It("returns an error", func() {
				Ω(returnedError).Should(Equal(expectedError))
			})
		})
	})
})
