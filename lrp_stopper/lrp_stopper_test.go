package lrp_stopper_test

import (
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
		cellID    string
		stopper   LRPStopper
		bbs       *fake_bbs.FakeRepBBS
		client    *fake_client.FakeClient
		logger    lager.Logger
		actualLRP models.ActualLRP
	)

	BeforeEach(func() {
		cellID = "the-cell-id"
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

		stopper = New(cellID, client, logger)
	})

	Describe("StopInstance", func() {
		var returnedError error

		JustBeforeEach(func() {
			returnedError = stopper.StopInstance(actualLRP.ProcessGuid, actualLRP.InstanceGuid)
		})

		It("succeeds", func() {
			Ω(returnedError).ShouldNot(HaveOccurred())
		})

		Context("when the executor returns 'not found'", func() {
			BeforeEach(func() {
				client.StopContainerReturns(executor.ErrContainerNotFound)
			})

			It("succeeds because the container is apparently already gone", func() {
				Ω(returnedError).ShouldNot(HaveOccurred())
			})
		})
	})
})
