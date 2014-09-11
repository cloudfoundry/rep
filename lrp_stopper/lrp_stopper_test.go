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
		executorID   string = "the-executor-id"
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

		stopper = New(executorID, bbs, client, logger)
	})

	Context("when told to stop an instance", func() {
		var returnedError error

		JustBeforeEach(func() {
			returnedError = stopper.StopInstance(stopInstance)
		})

		Context("when the instance is running on the executor", func() {
			BeforeEach(func() {
				bbs.GetActualLRPsByProcessGuidReturns([]models.ActualLRP{
					{
						ProcessGuid:  stopInstance.ProcessGuid,
						InstanceGuid: "some-other-instance-guid",
						Index:        1234,
						ExecutorID:   "some-other-executor-id",
					},
					{
						ProcessGuid:  stopInstance.ProcessGuid,
						InstanceGuid: stopInstance.InstanceGuid,
						Index:        stopInstance.Index,
						ExecutorID:   executorID,
					},
				}, nil)
			})

			It("attempts to delete the container", func() {
				Ω(client.DeleteContainerCallCount()).Should(Equal(1))

				allocationGuid := client.DeleteContainerArgsForCall(0)
				Ω(allocationGuid).Should(Equal(stopInstance.LRPIdentifier().OpaqueID()))
			})

			It("marks the LRP as stopped", func() {
				processGuid, index, instanceGuid := bbs.RemoveActualLRPForIndexArgsForCall(0)
				Ω(processGuid).Should(Equal(stopInstance.ProcessGuid))
				Ω(index).Should(Equal(stopInstance.Index))
				Ω(instanceGuid).Should(Equal(stopInstance.InstanceGuid))
			})

			It("deletes the stop command from the queue", func() {
				Ω(bbs.ResolveStopLRPInstanceArgsForCall(0)).Should(Equal(stopInstance))
			})

			It("succeeds", func() {
				Ω(returnedError).ShouldNot(HaveOccurred())
			})

			Context("when resolving the stop command fails", func() {
				BeforeEach(func() {
					bbs.ResolveStopLRPInstanceReturns(errors.New("oops"))
				})

				It("does not attempt to delete the container", func() {
					Ω(client.DeleteContainerCallCount()).Should(Equal(0))
				})

				It("returns an error", func() {
					Ω(returnedError).Should(MatchError(errors.New("oops")))
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

			Context("when the executor returns 'not found'", func() {
				BeforeEach(func() {
					client.DeleteContainerReturns(api.ErrContainerNotFound)
				})

				It("marks the LRP as stopped", func() {
					processGuid, index, instanceGuid := bbs.RemoveActualLRPForIndexArgsForCall(0)
					Ω(processGuid).Should(Equal(stopInstance.ProcessGuid))
					Ω(index).Should(Equal(stopInstance.Index))
					Ω(instanceGuid).Should(Equal(stopInstance.InstanceGuid))
				})

				It("succeeds because the container is apparently already gone", func() {
					Ω(returnedError).ShouldNot(HaveOccurred())
				})

				It("deletes the stop command from the queue", func() {
					Ω(bbs.ResolveStopLRPInstanceArgsForCall(0)).Should(Equal(stopInstance))
				})
			})

			Context("when marking the LRP as stopped fails", func() {
				var expectedError = errors.New("ker-blewie")

				BeforeEach(func() {
					bbs.RemoveActualLRPForIndexReturns(expectedError)
				})

				It("returns an error", func() {
					Ω(returnedError).Should(Equal(expectedError))
				})
			})
		})

		Context("when the instance is not running on the executor", func() {
			BeforeEach(func() {
				bbs.GetActualLRPsByProcessGuidReturns([]models.ActualLRP{
					{
						ProcessGuid:  stopInstance.ProcessGuid,
						InstanceGuid: "some-other-instance-guid",
						Index:        1234,
						ExecutorID:   "some-other-executor-id",
					},
				}, nil)
			})

			It("does not attempt to delete the container", func() {
				Ω(client.DeleteContainerCallCount()).Should(Equal(0))
			})

			It("does not mark the LRP as stopped", func() {
				Ω(bbs.ResolveStopLRPInstanceCallCount()).Should(Equal(0))
			})

			It("succeeds (as a noop)", func() {
				Ω(returnedError).ShouldNot(HaveOccurred())
			})
		})

		Context("when getting the actual LRPs for the stop instance fails", func() {
			BeforeEach(func() {
				bbs.GetActualLRPsByProcessGuidReturns(nil, errors.New("store is down"))
			})

			It("returns an error", func() {
				Ω(returnedError).Should(Equal(errors.New("store is down")))
			})

			It("doesn't resolve the stop command", func() {
				Ω(bbs.ResolveStopLRPInstanceCallCount()).Should(Equal(0))
			})

			It("doesn't delete the container", func() {
				Ω(client.DeleteContainerCallCount()).Should(Equal(0))
			})
		})
	})
})
