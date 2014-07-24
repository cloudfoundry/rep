package auction_delegate_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/executor/api"
	fake_client "github.com/cloudfoundry-incubator/executor/api/fakes"
	. "github.com/cloudfoundry-incubator/rep/auction_delegate"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper/fake_lrp_stopper"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AuctionDelegate", func() {
	var delegate auctiontypes.AuctionRepDelegate
	var client *fake_client.FakeClient
	var clientFetchError error
	var bbs *fake_bbs.FakeRepBBS
	var stopper *fake_lrp_stopper.FakeLRPStopper

	BeforeEach(func() {
		stopper = &fake_lrp_stopper.FakeLRPStopper{}
		client = new(fake_client.FakeClient)
		bbs = &fake_bbs.FakeRepBBS{}
		delegate = New("some-executor-id", stopper, bbs, client, lagertest.NewTestLogger("test"))
		clientFetchError = errors.New("Failed to fetch")
	})

	Describe("Remaining Resources", func() {
		Context("when the client returns a succesful response", func() {
			BeforeEach(func() {
				resources := api.ExecutorResources{
					MemoryMB:   1024,
					DiskMB:     2048,
					Containers: 4,
				}
				client.RemainingResourcesReturns(resources, nil)
			})

			It("Should use the client to get the resources", func() {
				resources, err := delegate.RemainingResources()
				Ω(err).ShouldNot(HaveOccurred())
				Ω(resources).Should(Equal(auctiontypes.Resources{
					MemoryMB:   1024,
					DiskMB:     2048,
					Containers: 4,
				}))
			})
		})

		Context("when the client returns an error", func() {
			BeforeEach(func() {
				client.RemainingResourcesReturns(api.ExecutorResources{}, clientFetchError)
			})

			It("should return the error", func() {
				_, err := delegate.RemainingResources()
				Ω(err).Should(Equal(clientFetchError))
			})
		})
	})

	Describe("Total Resources", func() {
		Context("when the client returns a succesful response", func() {
			BeforeEach(func() {
				resources := api.ExecutorResources{
					MemoryMB:   1024,
					DiskMB:     2048,
					Containers: 4,
				}
				client.TotalResourcesReturns(resources, nil)
			})

			It("Should use the client to get the resources", func() {
				resources, err := delegate.TotalResources()
				Ω(err).ShouldNot(HaveOccurred())
				Ω(resources).Should(Equal(auctiontypes.Resources{
					MemoryMB:   1024,
					DiskMB:     2048,
					Containers: 4,
				}))
			})
		})

		Context("when the client returns an error", func() {
			BeforeEach(func() {
				client.TotalResourcesReturns(api.ExecutorResources{}, clientFetchError)
			})

			It("should return the error", func() {
				_, err := delegate.TotalResources()
				Ω(err).Should(Equal(clientFetchError))
			})
		})
	})

	Describe("NumInstancesForProcessGuid", func() {
		Context("when the client returns a succesful response", func() {
			BeforeEach(func() {
				containers := []api.Container{
					api.Container{
						Guid: "the-first-app-guid.17.first",
					},
					api.Container{
						Guid: "the-second-app-guid.14.second",
					},
					api.Container{
						Guid: "the-first-app-guid.92.third",
					},
				}
				client.ListContainersReturns(containers, nil)
			})

			It("Should use the client to get the resources", func() {
				instances, err := delegate.NumInstancesForProcessGuid("the-first-app-guid")
				Ω(err).ShouldNot(HaveOccurred())
				Ω(instances).Should(Equal(2))

				instances, err = delegate.NumInstancesForProcessGuid("the-second-app-guid")
				Ω(err).ShouldNot(HaveOccurred())
				Ω(instances).Should(Equal(1))
			})

			Context("when there are no matching app guids", func() {
				It("should return 0", func() {
					instances, err := delegate.NumInstancesForProcessGuid("nope")
					Ω(err).ShouldNot(HaveOccurred())
					Ω(instances).Should(Equal(0))
				})
			})
		})

		Context("when the client returns an error", func() {
			BeforeEach(func() {
				client.ListContainersReturns([]api.Container{}, clientFetchError)
			})

			It("should return the error", func() {
				_, err := delegate.NumInstancesForProcessGuid("foo")
				Ω(err).Should(Equal(clientFetchError))
			})
		})
	})

	Describe("InstanceGuidsForProcessGuidAndIndex", func() {
		Context("when the client returns a succesful response", func() {
			BeforeEach(func() {
				containers := []api.Container{
					api.Container{
						Guid: "requested-app-guid.17.first",
					},
					api.Container{
						Guid: "requested-app-guid.17.second",
					},
					api.Container{
						Guid: "requested-app-guid.18.third",
					},
					api.Container{
						Guid: "other-app-guid.17.fourth",
					},
				}
				client.ListContainersReturns(containers, nil)
			})

			It("should return the instance guids", func() {
				instanceGuids, err := delegate.InstanceGuidsForProcessGuidAndIndex("requested-app-guid", 17)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(instanceGuids).Should(HaveLen(2))
				Ω(instanceGuids).Should(ContainElement("first"))
				Ω(instanceGuids).Should(ContainElement("second"))
			})

			Context("when there are no matching app guids", func() {
				It("should return empty", func() {
					instanceGuids, err := delegate.InstanceGuidsForProcessGuidAndIndex("nope", 17)
					Ω(err).ShouldNot(HaveOccurred())
					Ω(instanceGuids).Should(BeEmpty())
				})
			})

			Context("when there are no matching indexes", func() {
				It("should return empty", func() {
					instanceGuids, err := delegate.InstanceGuidsForProcessGuidAndIndex("requested-app-guid", 19)
					Ω(err).ShouldNot(HaveOccurred())
					Ω(instanceGuids).Should(BeEmpty())
				})
			})
		})

		Context("when the client returns an error", func() {
			BeforeEach(func() {
				client.ListContainersReturns([]api.Container{}, clientFetchError)
			})

			It("should return the error", func() {
				instanceGuids, err := delegate.InstanceGuidsForProcessGuidAndIndex("requested-app-guid", 17)
				Ω(err).Should(Equal(clientFetchError))
				Ω(instanceGuids).Should(BeEmpty())
			})
		})
	})

	Describe("Reserve", func() {
		var reserveErr error
		var auctionInfo auctiontypes.StartAuctionInfo

		JustBeforeEach(func() {
			reserveErr = delegate.Reserve(auctionInfo)
		})

		Context("when the client returns a succesful response", func() {

			BeforeEach(func() {
				auctionInfo = auctiontypes.StartAuctionInfo{
					ProcessGuid:  "process-guid",
					InstanceGuid: "instance-guid",
					DiskMB:       1024,
					MemoryMB:     2048,
					Index:        17,
				}

				client.AllocateContainerReturns(api.Container{}, nil)
			})

			It("should not return an error", func() {
				Ω(reserveErr).ShouldNot(HaveOccurred())
			})

			It("should allocate a container, passing in the correct data", func() {
				Ω(client.AllocateContainerCallCount()).Should(Equal(1))

				allocationGuid, req := client.AllocateContainerArgsForCall(0)
				Ω(allocationGuid).Should(Equal(auctionInfo.LRPIdentifier().OpaqueID()))
				Ω(req).Should(Equal(api.ContainerAllocationRequest{
					MemoryMB: auctionInfo.MemoryMB,
					DiskMB:   auctionInfo.DiskMB,
				}))
			})
		})

		Context("when the client returns an error", func() {
			BeforeEach(func() {
				client.AllocateContainerReturns(api.Container{}, clientFetchError)
			})

			It("should return the error", func() {
				Ω(reserveErr).Should(Equal(clientFetchError))
			})
		})
	})

	Describe("ReleaseReservation", func() {
		var auctionInfo auctiontypes.StartAuctionInfo
		var releaseErr error

		JustBeforeEach(func() {
			auctionInfo = auctiontypes.StartAuctionInfo{
				ProcessGuid:  "process-guid",
				InstanceGuid: "instance-guid",
				DiskMB:       1024,
				MemoryMB:     2048,
				Index:        17,
			}
			releaseErr = delegate.ReleaseReservation(auctionInfo)
		})

		Context("when the client returns a succesful response", func() {
			BeforeEach(func() {
				client.DeleteContainerReturns(nil)
			})

			It("should not return an error", func() {
				Ω(releaseErr).ShouldNot(HaveOccurred())
			})

			It("should allocate a container, passing in the correct data", func() {
				Ω(client.DeleteContainerCallCount()).Should(Equal(1))
				Ω(client.DeleteContainerArgsForCall(0)).Should(Equal(auctionInfo.LRPIdentifier().OpaqueID()))
			})
		})

		Context("when the client returns an error", func() {
			BeforeEach(func() {
				client.DeleteContainerReturns(clientFetchError)
			})

			It("should return the error", func() {
				Ω(releaseErr).Should(Equal(clientFetchError))
			})
		})
	})

	Describe("Run", func() {
		var startAuction models.LRPStartAuction
		var err, initializeError, startingErr, runError error

		BeforeEach(func() {
			initializeError, startingErr, runError = nil, nil, nil

			startAuction = models.LRPStartAuction{
				ProcessGuid:  "process-guid",
				InstanceGuid: "instance-guid",
				Actions: []models.ExecutorAction{
					{
						Action: models.DownloadAction{
							From: "http://example.com/something",
							To:   "/something",
						},
					},
				},
				Log: models.LogConfig{Guid: "log-guid"},
				Ports: []models.PortMapping{
					{ContainerPort: 8080},
				},
				Index: 2,
			}
		})

		JustBeforeEach(func() {
			client.InitializeContainerReturns(api.Container{}, initializeError)
			bbs.ReportActualLRPAsStartingReturns(startingErr)
			client.RunReturns(runError)
			client.DeleteContainerReturns(nil)

			err = delegate.Run(startAuction)
		})

		Context("when running succeeds", func() {
			It("should not return an error", func() {
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("should attempt to initialize the correct container", func() {
				Ω(client.InitializeContainerCallCount()).Should(Equal(1))
				allocationGuid, initRequest := client.InitializeContainerArgsForCall(0)
				Ω(allocationGuid).Should(Equal(startAuction.LRPIdentifier().OpaqueID()))
				Ω(initRequest).Should(Equal(api.ContainerInitializationRequest{
					Ports: []api.PortMapping{{ContainerPort: 8080}},
					Log:   models.LogConfig{Guid: "log-guid"},
				}))
			})

			It("should mark the instance as STARTING in etcd", func() {
				Ω(bbs.ReportActualLRPAsStartingCallCount()).Should(Equal(1))
				actualLRP, executorGuid := bbs.ReportActualLRPAsStartingArgsForCall(0)
				Ω(actualLRP).Should(Equal(models.ActualLRP{
					ProcessGuid:  startAuction.ProcessGuid,
					InstanceGuid: startAuction.InstanceGuid,
					Index:        startAuction.Index,
				}))
				Ω(executorGuid).Should(Equal("some-executor-id"))
			})

			It("should attempt to run the correct actions in the correct container", func() {
				Ω(client.RunCallCount()).Should(Equal(1))

				allocationGuid, runRequest := client.RunArgsForCall(0)

				Ω(allocationGuid).Should(Equal(startAuction.LRPIdentifier().OpaqueID()))
				Ω(runRequest).Should(Equal(api.ContainerRunRequest{
					Actions: startAuction.Actions,
				}))
			})

			It("does not attempt to remove the STARTING LRP from etcd", func() {
				Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(0))
			})

			It("should not delete the container", func() {
				Ω(client.DeleteContainerCallCount()).Should(Equal(0))
			})
		})

		Context("when the initialize fails", func() {
			BeforeEach(func() {
				initializeError = errors.New("Failed to initialize")
			})

			It("should bubble up the error", func() {
				Ω(err).Should(Equal(initializeError))
			})

			It("should attempt to initialize the correct container", func() {
				Ω(client.InitializeContainerCallCount()).Should(Equal(1))
				allocationGuid, initRequest := client.InitializeContainerArgsForCall(0)
				Ω(allocationGuid).Should(Equal(startAuction.LRPIdentifier().OpaqueID()))
				Ω(initRequest).Should(Equal(api.ContainerInitializationRequest{
					Ports: []api.PortMapping{{ContainerPort: 8080}},
					Log:   models.LogConfig{Guid: "log-guid"},
				}))
			})

			It("should not mark the task as starting", func() {
				Ω(bbs.ReportActualLRPAsStartingCallCount()).Should(Equal(0))
			})

			It("should not attempt to run the actions", func() {
				Ω(client.RunCallCount()).Should(Equal(0))
			})

			It("should delete the container", func() {
				Ω(client.DeleteContainerCallCount()).Should(Equal(1))
				allocationGuid := client.DeleteContainerArgsForCall(0)
				Ω(allocationGuid).Should(Equal(startAuction.LRPIdentifier().OpaqueID()))
			})
		})

		Context("when marking the instance as STARTING fails", func() {
			BeforeEach(func() {
				startingErr = errors.New("kaboom")
			})

			It("should bubble up the error", func() {
				Ω(err).Should(Equal(startingErr))
			})

			It("should attempt to initialize the correct container", func() {
				Ω(client.InitializeContainerCallCount()).Should(Equal(1))
				allocationGuid, initRequest := client.InitializeContainerArgsForCall(0)
				Ω(allocationGuid).Should(Equal(startAuction.LRPIdentifier().OpaqueID()))
				Ω(initRequest).Should(Equal(api.ContainerInitializationRequest{
					Ports: []api.PortMapping{{ContainerPort: 8080}},
					Log:   models.LogConfig{Guid: "log-guid"},
				}))
			})

			It("should mark the instance as STARTING in etcd", func() {
				Ω(bbs.ReportActualLRPAsStartingCallCount()).Should(Equal(1))
				actualLRP, executorGuid := bbs.ReportActualLRPAsStartingArgsForCall(0)
				Ω(actualLRP).Should(Equal(models.ActualLRP{
					ProcessGuid:  startAuction.ProcessGuid,
					InstanceGuid: startAuction.InstanceGuid,
					Index:        startAuction.Index,
				}))
				Ω(executorGuid).Should(Equal("some-executor-id"))
			})

			It("does not attempt to remove the STARTING LRP from etcd", func() {
				Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(0))
			})

			It("should not attempt to run the actions", func() {
				Ω(client.RunCallCount()).Should(Equal(0))
			})

			It("should delete the container", func() {
				Ω(client.DeleteContainerCallCount()).Should(Equal(1))
				allocationGuid := client.DeleteContainerArgsForCall(0)
				Ω(allocationGuid).Should(Equal(startAuction.LRPIdentifier().OpaqueID()))
			})
		})

		Context("when run fails", func() {
			BeforeEach(func() {
				runError = errors.New("Failed to run")
			})

			It("should bubble up the error", func() {
				Ω(err).Should(Equal(runError))
			})

			It("should attempt to initialize the correct container", func() {
				Ω(client.InitializeContainerCallCount()).Should(Equal(1))
				allocationGuid, initRequest := client.InitializeContainerArgsForCall(0)
				Ω(allocationGuid).Should(Equal(startAuction.LRPIdentifier().OpaqueID()))
				Ω(initRequest).Should(Equal(api.ContainerInitializationRequest{
					Ports: []api.PortMapping{{ContainerPort: 8080}},
					Log:   models.LogConfig{Guid: "log-guid"},
				}))
			})

			It("should mark the instance as STARTING in etcd", func() {
				Ω(bbs.ReportActualLRPAsStartingCallCount()).Should(Equal(1))
				actualLRP, executorGuid := bbs.ReportActualLRPAsStartingArgsForCall(0)
				Ω(actualLRP).Should(Equal(models.ActualLRP{
					ProcessGuid:  startAuction.ProcessGuid,
					InstanceGuid: startAuction.InstanceGuid,
					Index:        startAuction.Index,
				}))
				Ω(executorGuid).Should(Equal("some-executor-id"))
			})

			It("should remove the STARTING LRP from etcd", func() {
				Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(1))
				Ω(bbs.RemoveActualLRPArgsForCall(0).InstanceGuid).Should(Equal(startAuction.InstanceGuid))
			})

			It("should attempt to run the actions in the correct container", func() {
				Ω(client.RunCallCount()).Should(Equal(1))

				allocationGuid, runRequest := client.RunArgsForCall(0)

				Ω(allocationGuid).Should(Equal(startAuction.LRPIdentifier().OpaqueID()))
				Ω(runRequest).Should(Equal(api.ContainerRunRequest{
					Actions: startAuction.Actions,
				}))
			})

			It("should delete the container", func() {
				Ω(client.DeleteContainerCallCount()).Should(Equal(1))
				allocationGuid := client.DeleteContainerArgsForCall(0)
				Ω(allocationGuid).Should(Equal(startAuction.LRPIdentifier().OpaqueID()))
			})
		})
	})

	Describe("Stop", func() {
		It("should instruct the LRPStopper to stop", func() {
			stopInstance := models.StopLRPInstance{
				ProcessGuid:  "some-process-guid",
				InstanceGuid: "some-instance-guid",
				Index:        2,
			}

			delegate.Stop(stopInstance)
			Ω(stopper.StopInstanceArgsForCall(0)).Should(Equal(stopInstance))
		})
	})
})
