package lrp_stopper_test

import (
	"errors"

	fake_client "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

var _ = Describe("LRP Stopper", func() {
	var (
		cellID    string
		stopper   lrp_stopper.LRPStopper
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
			ActualLRPInstanceKey: models.NewActualLRPInstanceKey(
				"some-instance-guid",
				"some-cell-id",
			),
		}

		client = new(fake_client.FakeClient)
		logger = lagertest.NewTestLogger("test")

		stopper = lrp_stopper.New(cellID, client, logger)
	})

	Describe("StopInstance", func() {
		var returnedError error

		JustBeforeEach(func() {
			returnedError = stopper.StopInstance(actualLRP.ProcessGuid, actualLRP.InstanceGuid)
		})

		It("succeeds", func() {
			Expect(returnedError).NotTo(HaveOccurred())
		})

		Context("when the executor returns an unexpected error", func() {
			BeforeEach(func() {
				client.StopContainerReturns(errors.New("use of closed network connection"))
			})

			It("returns an error", func() {
				Expect(returnedError).To(HaveOccurred())
				Expect(returnedError.Error()).To(ContainSubstring("use of closed network connection"))
			})
		})
	})
})
