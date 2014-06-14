package auction_delegate_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/executor/client/fake_client"
	. "github.com/cloudfoundry-incubator/rep/auction_delegate"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper/fake_lrp_stopper"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	steno "github.com/cloudfoundry/gosteno"

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
		client = fake_client.New()
		bbs = fake_bbs.NewFakeRepBBS()
		delegate = New(stopper, bbs, client, steno.NewLogger("test"))
		clientFetchError = errors.New("Failed to fetch")
	})

	Describe("Remaining Resources", func() {
		Context("when the client returns a succesful response", func() {
			BeforeEach(func() {
				client.WhenFetchingRemainingResources = func() (api.ExecutorResources, error) {
					return api.ExecutorResources{
						MemoryMB:   1024,
						DiskMB:     2048,
						Containers: 4,
					}, nil
				}
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
				client.WhenFetchingRemainingResources = func() (api.ExecutorResources, error) {
					return api.ExecutorResources{}, clientFetchError
				}
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
				client.WhenFetchingTotalResources = func() (api.ExecutorResources, error) {
					return api.ExecutorResources{
						MemoryMB:   1024,
						DiskMB:     2048,
						Containers: 4,
					}, nil
				}
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
				client.WhenFetchingTotalResources = func() (api.ExecutorResources, error) {
					return api.ExecutorResources{}, clientFetchError
				}
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
				client.WhenListingContainers = func() ([]api.Container, error) {
					return []api.Container{
						api.Container{
							Guid: "first",
							Metadata: map[string]string{
								ProcessGuidMetadataKey: "the-first-app-guid",
								IndexMetadataKey:       "17",
							},
						},
						api.Container{
							Guid: "second",
							Metadata: map[string]string{
								ProcessGuidMetadataKey: "the-second-app-guid",
								IndexMetadataKey:       "14",
							},
						},
						api.Container{
							Guid: "third",
							Metadata: map[string]string{
								ProcessGuidMetadataKey: "the-first-app-guid",
								IndexMetadataKey:       "92",
							},
						},
					}, nil
				}
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
				client.WhenListingContainers = func() ([]api.Container, error) {
					return []api.Container{}, clientFetchError
				}
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
				client.WhenListingContainers = func() ([]api.Container, error) {
					return []api.Container{
						api.Container{
							Guid: "first",
							Metadata: map[string]string{
								ProcessGuidMetadataKey: "requested-app-guid",
								IndexMetadataKey:       "17",
							},
						},
						api.Container{
							Guid: "second",
							Metadata: map[string]string{
								ProcessGuidMetadataKey: "requested-app-guid",
								IndexMetadataKey:       "17",
							},
						},
						api.Container{
							Guid: "third",
							Metadata: map[string]string{
								ProcessGuidMetadataKey: "requested-app-guid",
								IndexMetadataKey:       "18",
							},
						},
						api.Container{
							Guid: "fourth",
							Metadata: map[string]string{
								ProcessGuidMetadataKey: "other-app-guid",
								IndexMetadataKey:       "17",
							},
						},
					}, nil
				}
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
				client.WhenListingContainers = func() ([]api.Container, error) {
					return []api.Container{}, clientFetchError
				}
			})

			It("should return the error", func() {
				instanceGuids, err := delegate.InstanceGuidsForProcessGuidAndIndex("requested-app-guid", 17)
				Ω(err).Should(Equal(clientFetchError))
				Ω(instanceGuids).Should(BeEmpty())
			})
		})
	})

	Describe("Reserve", func() {
		var auctionInfo auctiontypes.StartAuctionInfo
		var allocationCalled bool

		Context("when the client returns a succesful response", func() {

			BeforeEach(func() {
				allocationCalled = false
				auctionInfo = auctiontypes.StartAuctionInfo{
					ProcessGuid:  "process-guid",
					InstanceGuid: "instance-guid",
					DiskMB:       1024,
					MemoryMB:     2048,
					Index:        17,
				}

				client.WhenAllocatingContainer = func(allocationGuid string, req api.ContainerAllocationRequest) (api.Container, error) {
					allocationCalled = true
					Ω(allocationGuid).Should(Equal(auctionInfo.InstanceGuid))
					Ω(req).Should(Equal(api.ContainerAllocationRequest{
						MemoryMB: auctionInfo.MemoryMB,
						DiskMB:   auctionInfo.DiskMB,
						Metadata: map[string]string{ProcessGuidMetadataKey: auctionInfo.ProcessGuid, IndexMetadataKey: "17"},
					}))
					return api.Container{}, nil
				}
			})

			It("should allocate a container, passing in the correct data", func() {
				err := delegate.Reserve(auctionInfo)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(allocationCalled).Should(BeTrue())
			})
		})

		Context("when the client returns an error", func() {

			BeforeEach(func() {
				client.WhenAllocatingContainer = func(allocationGuid string, req api.ContainerAllocationRequest) (api.Container, error) {
					return api.Container{}, clientFetchError
				}
			})

			It("should return the error", func() {
				err := delegate.Reserve(auctionInfo)
				Ω(err).Should(Equal(clientFetchError))
			})
		})
	})

	Describe("ReleaseReservation", func() {
		var auctionInfo auctiontypes.StartAuctionInfo
		var releaseCalled bool

		Context("when the client returns a succesful response", func() {
			BeforeEach(func() {
				releaseCalled = false
				auctionInfo = auctiontypes.StartAuctionInfo{
					ProcessGuid:  "process-guid",
					InstanceGuid: "instance-guid",
					DiskMB:       1024,
					MemoryMB:     2048,
					Index:        17,
				}

				client.WhenDeletingContainer = func(allocationGuid string) error {
					releaseCalled = true
					Ω(allocationGuid).Should(Equal(auctionInfo.InstanceGuid))
					return nil
				}
			})

			It("should allocate a container, passing in the correct data", func() {
				err := delegate.ReleaseReservation(auctionInfo)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(releaseCalled).Should(BeTrue())
			})
		})

		Context("when the client returns an error", func() {
			BeforeEach(func() {
				client.WhenDeletingContainer = func(allocationGuid string) error {
					return clientFetchError
				}
			})

			It("should return the error", func() {
				err := delegate.ReleaseReservation(auctionInfo)
				Ω(err).Should(Equal(clientFetchError))
			})
		})
	})

	Describe("Run", func() {
		var startAuction models.LRPStartAuction
		var initializeError, runError error
		var calledInitialize, calledRun, deleteCalled bool
		var err error

		BeforeEach(func() {
			initializeError, runError = nil, nil
			calledInitialize, calledRun, deleteCalled = false, false, false

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

			client.WhenInitializingContainer = func(allocationGuid string, request api.ContainerInitializationRequest) (api.Container, error) {
				Ω(allocationGuid).Should(Equal(startAuction.InstanceGuid))
				Ω(request).Should(Equal(api.ContainerInitializationRequest{
					Ports: []api.PortMapping{
						{
							HostPort:      startAuction.Ports[0].HostPort,
							ContainerPort: startAuction.Ports[0].ContainerPort,
						},
					},
					Log: startAuction.Log,
				}))
				calledInitialize = true
				return api.Container{ExecutorGuid: "some-executor-id"}, initializeError
			}

			client.WhenRunning = func(allocationGuid string, request api.ContainerRunRequest) error {
				Ω(allocationGuid).Should(Equal(startAuction.InstanceGuid))
				Ω(request).Should(Equal(api.ContainerRunRequest{
					Actions: startAuction.Actions,
				}))
				calledRun = true
				return runError
			}

			client.WhenDeletingContainer = func(allocationGuid string) error {
				deleteCalled = true
				Ω(allocationGuid).Should(Equal(startAuction.InstanceGuid))
				return nil
			}
		})

		JustBeforeEach(func() {
			err = delegate.Run(startAuction)
		})

		Context("when the initialize succeeds", func() {
			BeforeEach(func() {
				initializeError = nil
			})

			It("should mark the instance as STARTING in etcd", func() {
				Ω(err).ShouldNot(HaveOccurred())
				Ω(bbs.StartingLRPs()).Should(HaveLen(1))
				Ω(bbs.StartingLRPs()[0]).Should(Equal(models.ActualLRP{
					ProcessGuid:  startAuction.ProcessGuid,
					InstanceGuid: startAuction.InstanceGuid,
					Index:        startAuction.Index,
				}))
				Ω(bbs.StartingLRPExecutorIDs()[0]).Should(Equal("some-executor-id"))
			})

			Context("when marking the instance as STARTING fails", func() {
				BeforeEach(func() {
					bbs.SetStartingError(errors.New("kaboom"))
				})

				It("should fail", func() {
					Ω(err).Should(Equal(errors.New("kaboom")))
				})

				It("should delete the container", func() {
					Ω(deleteCalled).Should(BeTrue())
				})

				It("should not have run", func() {
					Ω(calledRun).Should(BeFalse())
				})
			})

			Context("when run succeeds", func() {
				BeforeEach(func() {
					runError = nil
				})

				It("should succeed", func() {
					Ω(err).ShouldNot(HaveOccurred())
					Ω(calledInitialize).Should(BeTrue())
					Ω(calledRun).Should(BeTrue())
					Ω(deleteCalled).Should(BeFalse())
				})
			})

			Context("when run fails", func() {
				BeforeEach(func() {
					runError = errors.New("Failed to run")
				})

				It("should have remove the STARTING LRP from etcd", func() {
					Ω(bbs.StartingLRPs()).Should(HaveLen(1))
					Ω(bbs.RemovedLRPs()).Should(HaveLen(1))
				})

				It("should fail", func() {
					Ω(err).Should(Equal(runError))
					Ω(calledInitialize).Should(BeTrue())
					Ω(calledRun).Should(BeTrue())
				})

				It("should delete the container", func() {
					Ω(deleteCalled).Should(BeTrue())
				})
			})
		})

		Context("when the initialize fails", func() {
			BeforeEach(func() {
				initializeError = errors.New("Failed to initialize")
			})

			It("should not mark the task as starting", func() {
				Ω(err).Should(Equal(initializeError))

				Ω(bbs.StartingLRPs()).Should(BeEmpty())
			})

			It("should not call run and should return an error", func() {
				Ω(err).Should(Equal(initializeError))
				Ω(calledInitialize).Should(BeTrue())
				Ω(calledRun).Should(BeFalse())
			})

			It("should delete the container", func() {
				delegate.Run(startAuction)
				Ω(deleteCalled).Should(BeTrue())
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
