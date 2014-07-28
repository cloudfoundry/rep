package lrp_stopper_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/executor/api"
	fake_client "github.com/cloudfoundry-incubator/executor/api/fakes"
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
		stopper      LRPStopper
		bbs          *fake_bbs.FakeRepBBS
		client       *fake_client.FakeClient
		logger       lager.Logger
		stopInstance models.StopLRPInstance
	)

	BeforeEach(func() {
		stopInstance = models.StopLRPInstance{
			ProcessGuid:  "some-process-guid",
			InstanceGuid: "some-instance-guid",
			Index:        1138,
		}

		bbs = &fake_bbs.FakeRepBBS{}
		client = new(fake_client.FakeClient)
		logger = lagertest.NewTestLogger("test")

		stopper = New(bbs, client, logger)
	})

	Context("when told to stop an instance", func() {
		var returnedError error

		JustBeforeEach(func() {
			returnedError = stopper.StopInstance(stopInstance)
		})

		Context("when the instance is one that is running on the executor", func() {
			BeforeEach(func() {
				client.GetContainerReturns(api.Container{Guid: stopInstance.LRPIdentifier().OpaqueID()}, nil)
			})

			It("should ensure the container exists", func() {
				Ω(client.GetContainerCallCount()).Should(Equal(1))

				allocationGuid := client.GetContainerArgsForCall(0)
				Ω(allocationGuid).Should(Equal(stopInstance.LRPIdentifier().OpaqueID()))
			})

			It("should attempt to delete the container", func() {
				Ω(client.DeleteContainerCallCount()).Should(Equal(1))

				allocationGuid := client.DeleteContainerArgsForCall(0)
				Ω(allocationGuid).Should(Equal(stopInstance.LRPIdentifier().OpaqueID()))
			})

			It("should mark the LRP as stopped", func() {
				processGuid, index, instanceGuid := bbs.RemoveActualLRPForIndexArgsForCall(0)
				Ω(processGuid).Should(Equal(stopInstance.ProcessGuid))
				Ω(index).Should(Equal(stopInstance.Index))
				Ω(instanceGuid).Should(Equal(stopInstance.InstanceGuid))
			})

			It("deletes the stop command from the queue", func() {
				Ω(bbs.ResolveStopLRPInstanceArgsForCall(0)).Should(Equal(stopInstance))
			})

			It("should not error", func() {
				Ω(returnedError).ShouldNot(HaveOccurred())
			})
		})

		Context("when resolving the LRP fails", func() {
			BeforeEach(func() {
				client.GetContainerReturns(api.Container{Guid: stopInstance.LRPIdentifier().OpaqueID()}, nil)
				bbs.ResolveStopLRPInstanceReturns(errors.New("oops"))
			})

			It("should not attempt to delete the container", func() {
				Ω(client.DeleteContainerCallCount()).Should(Equal(0))
			})

			It("should bubble up the error", func() {
				Ω(returnedError).Should(MatchError(errors.New("oops")))
			})
		})

		Context("when the instance is not running on the executor", func() {
			BeforeEach(func() {
				client.GetContainerReturns(api.Container{}, errors.New("nope"))
			})

			It("should not attempt to delete the container", func() {
				Ω(client.DeleteContainerCallCount()).Should(Equal(0))
			})

			It("should not attempt to mark the LRP as stopped", func() {
				Ω(bbs.ResolveStopLRPInstanceCallCount()).Should(Equal(0))
			})

			It("should bubble up the error", func() {
				Ω(returnedError).Should(MatchError(errors.New("nope")))
			})
		})

		Context("when deleting the container fails", func() {
			BeforeEach(func() {
				client.DeleteContainerReturns(errors.New("Couldn't delete that"))
			})

			It("does not mark the LRP as stopped", func() {
				Ω(bbs.RemoveActualLRPForIndexCallCount()).Should(Equal(0))
			})
		})

		Context("when marking the LRP as stopped fails", func() {
			var expectedError = errors.New("ker-blewie")

			BeforeEach(func() {
				bbs.RemoveActualLRPForIndexReturns(expectedError)
			})

			It("bubbles uo the error", func() {
				Ω(returnedError).Should(Equal(expectedError))
			})
		})
	})
})
