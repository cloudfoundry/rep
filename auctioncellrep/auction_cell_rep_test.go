package auctioncellrep_test

import (
	"errors"
	"fmt"

	"code.cloudfoundry.org/bbs/models"
	"code.cloudfoundry.org/executor"
	fake_client "code.cloudfoundry.org/executor/fakes"
	"code.cloudfoundry.org/lager/lagertest"
	"code.cloudfoundry.org/rep"
	"code.cloudfoundry.org/rep/auctioncellrep"
	"code.cloudfoundry.org/rep/evacuation/evacuation_context/fake_evacuation_context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AuctionCellRep", func() {
	const (
		repURL     = "https://foo.cell.service.cf.internal:8888"
		cellID     = "some-cell-id"
		linuxStack = "linux"
		linuxPath  = "/data/rootfs/linux"
	)

	var (
		cellRep            auctioncellrep.AuctionCellClient
		client             *fake_client.FakeClient
		logger             *lagertest.TestLogger
		evacuationReporter *fake_evacuation_context.FakeEvacuationReporter

		expectedGuid, linuxRootFSURL string
		commonErr, expectedGuidError error

		fakeGenerateContainerGuid func() (string, error)

		placementTags, optionalPlacementTags []string
		proxyMemoryAllocation                int
		enableContainerProxy                 bool
	)

	BeforeEach(func() {
		client = new(fake_client.FakeClient)
		logger = lagertest.NewTestLogger("test")
		evacuationReporter = &fake_evacuation_context.FakeEvacuationReporter{}

		expectedGuid = "container-guid"
		expectedGuidError = nil
		fakeGenerateContainerGuid = func() (string, error) {
			return expectedGuid, expectedGuidError
		}
		linuxRootFSURL = models.PreloadedRootFS(linuxStack)

		commonErr = errors.New("Failed to fetch")
		proxyMemoryAllocation = 0
		enableContainerProxy = false
		client.HealthyReturns(true)
	})

	JustBeforeEach(func() {
		cellRep = auctioncellrep.New(
			cellID,
			repURL,
			rep.StackPathMap{linuxStack: linuxPath},
			[]string{"docker"},
			"the-zone",
			fakeGenerateContainerGuid,
			client,
			evacuationReporter,
			placementTags,
			optionalPlacementTags,
			proxyMemoryAllocation,
			enableContainerProxy,
		)
	})

	Describe("State", func() {
		var (
			availableResources, totalResources executor.ExecutorResources
			containers                         []executor.Container
			volumeDrivers                      []string
		)

		BeforeEach(func() {
			evacuationReporter.EvacuatingReturns(true)
			totalResources = executor.ExecutorResources{
				MemoryMB:   1024,
				DiskMB:     2048,
				Containers: 4,
			}

			availableResources = executor.ExecutorResources{
				MemoryMB:   512,
				DiskMB:     256,
				Containers: 2,
			}

			containers = []executor.Container{
				{
					Guid:     "first",
					Resource: executor.NewResource(20, 10, 100, linuxPath),
					Tags: executor.Tags{
						rep.LifecycleTag:     rep.LRPLifecycle,
						rep.ProcessGuidTag:   "the-first-app-guid",
						rep.ProcessIndexTag:  "17",
						rep.DomainTag:        "domain",
						rep.InstanceGuidTag:  "ig-1",
						rep.PlacementTagsTag: `["pt-1"]`,
						rep.VolumeDriversTag: `["vd-1"]`,
					},
					State: executor.StateReserved,
				},
				{
					Guid:     "second",
					Resource: executor.NewResource(40, 30, 100, "docker://onsi/grace-busybox"),
					Tags: executor.Tags{
						rep.LifecycleTag:     rep.LRPLifecycle,
						rep.ProcessGuidTag:   "the-second-app-guid",
						rep.ProcessIndexTag:  "92",
						rep.DomainTag:        "domain",
						rep.InstanceGuidTag:  "ig-2",
						rep.PlacementTagsTag: `["pt-2"]`,
						rep.VolumeDriversTag: `["vd-2"]`,
					},
					State: executor.StateInitializing,
				},
				{
					Guid: "da-task",
					Resource: executor.NewResource(
						40,
						30,
						100,
						fmt.Sprintf("%s:%s?key=value", models.PreloadedOCIRootFSScheme, linuxPath),
					),
					Tags: executor.Tags{
						rep.LifecycleTag:     rep.TaskLifecycle,
						rep.DomainTag:        "domain",
						rep.PlacementTagsTag: `[]`,
						rep.VolumeDriversTag: `[]`,
					},
					State: executor.StateCreated,
				},
				{
					Guid:     "another-task",
					Resource: executor.NewResource(40, 30, 100, ":url-error"),
					Tags: executor.Tags{
						rep.LifecycleTag:     rep.TaskLifecycle,
						rep.DomainTag:        "domain",
						rep.PlacementTagsTag: `[]`,
						rep.VolumeDriversTag: `[]`,
					},
					State: executor.StateRunning,
				},
				{
					Guid:     "other-task",
					Resource: executor.NewResource(40, 30, 100, linuxPath),
					Tags:     nil,
					State:    executor.StateRunning,
				},
			}

			volumeDrivers = []string{"lewis", "nico", "sebastian", "felipe"}
			placementTags = []string{}
			optionalPlacementTags = []string{}

			client.TotalResourcesReturns(totalResources, nil)
			client.RemainingResourcesReturns(availableResources, nil)
			client.ListContainersReturns(containers, nil)
			client.VolumeDriversReturns(volumeDrivers, nil)
		})

		It("queries the client and returns state", func() {
			state, healthy, err := cellRep.State(logger)
			Expect(err).NotTo(HaveOccurred())

			Expect(healthy).To(BeTrue())

			Expect(state.CellID).To(Equal(cellID))
			Expect(state.RepURL).To(Equal(repURL))

			Expect(state.Evacuating).To(BeTrue())
			Expect(state.RootFSProviders).To(Equal(rep.RootFSProviders{
				models.PreloadedRootFSScheme:    rep.NewFixedSetRootFSProvider("linux"),
				models.PreloadedOCIRootFSScheme: rep.NewFixedSetRootFSProvider("linux"),
				"docker":                        rep.ArbitraryRootFSProvider{},
			}))

			Expect(state.AvailableResources).To(Equal(rep.Resources{
				MemoryMB:   int32(availableResources.MemoryMB),
				DiskMB:     int32(availableResources.DiskMB),
				Containers: availableResources.Containers,
			}))

			Expect(state.TotalResources).To(Equal(rep.Resources{
				MemoryMB:   int32(totalResources.MemoryMB),
				DiskMB:     int32(totalResources.DiskMB),
				Containers: totalResources.Containers,
			}))

			Expect(state.LRPs).To(ConsistOf([]rep.LRP{
				rep.NewLRP(
					"ig-1",
					models.NewActualLRPKey("the-first-app-guid", 17, "domain"),
					rep.NewResource(20, 10, 100),
					rep.NewPlacementConstraint("preloaded:linux", []string{"pt-1"}, []string{"vd-1"}),
				),
				rep.NewLRP(
					"ig-2",
					models.NewActualLRPKey("the-second-app-guid", 92, "domain"),
					rep.NewResource(40, 30, 100),
					rep.NewPlacementConstraint("docker://onsi/grace-busybox", []string{"pt-2"}, []string{"vd-2"}),
				),
			}))

			Expect(state.Tasks).To(ConsistOf([]rep.Task{
				rep.NewTask(
					"da-task",
					"domain",
					rep.NewResource(40, 30, 100),
					rep.NewPlacementConstraint("preloaded+layer:linux?key=value", []string{}, []string{}),
				),
				rep.NewTask(
					"another-task",
					"domain",
					rep.NewResource(40, 30, 100),
					rep.NewPlacementConstraint(":url-error", []string{}, []string{}),
				),
			}))

			Expect(state.StartingContainerCount).To(Equal(3))

			Expect(state.VolumeDrivers).To(ConsistOf(volumeDrivers))
		})

		Context("when the cell is not healthy", func() {
			BeforeEach(func() {
				client.HealthyReturns(false)
			})

			It("errors when reporting state", func() {
				_, healthy, err := cellRep.State(logger)
				Expect(err).NotTo(HaveOccurred())
				Expect(healthy).To(BeFalse())
			})
		})

		Context("when the client fails to fetch total resources", func() {
			BeforeEach(func() {
				client.TotalResourcesReturns(executor.ExecutorResources{}, commonErr)
			})

			It("should return an error and no state", func() {
				_, _, err := cellRep.State(logger)
				Expect(err).To(MatchError(commonErr))
			})
		})

		Context("when the client fails to fetch available resources", func() {
			BeforeEach(func() {
				client.RemainingResourcesReturns(executor.ExecutorResources{}, commonErr)
			})

			It("should return an error and no state", func() {
				_, _, err := cellRep.State(logger)
				Expect(err).To(MatchError(commonErr))
			})
		})

		Context("when the client fails to list containers", func() {
			BeforeEach(func() {
				client.ListContainersReturns(nil, commonErr)
			})

			It("should return an error and no state", func() {
				_, _, err := cellRep.State(logger)
				Expect(err).To(MatchError(commonErr))
			})
		})

		Context("when placement tags have been set", func() {
			BeforeEach(func() {
				placementTags = []string{"quack", "oink"}
			})

			It("returns the tags as part of the state", func() {
				state, healthy, err := cellRep.State(logger)
				Expect(err).NotTo(HaveOccurred())
				Expect(healthy).To(BeTrue())
				Expect(state.PlacementTags).To(ConsistOf(placementTags))
			})
		})

		Context("when optional placement tags have been set", func() {
			BeforeEach(func() {
				optionalPlacementTags = []string{"baa", "cluck"}
			})

			It("returns the tags as part of the state", func() {
				state, healthy, err := cellRep.State(logger)
				Expect(err).NotTo(HaveOccurred())
				Expect(healthy).To(BeTrue())
				Expect(state.OptionalPlacementTags).To(ConsistOf(optionalPlacementTags))
			})
		})
	})

	Describe("Perform", func() {
		var (
			work rep.Work
			task rep.Task

			expectedIndex = 1
		)

		BeforeEach(func() {
			client.RemainingResourcesReturns(executor.ExecutorResources{MemoryMB: 8192}, nil)
		})

		Context("when evacuating", func() {
			BeforeEach(func() {
				evacuationReporter.EvacuatingReturns(true)

				lrp := rep.NewLRP(
					"ig-1",
					models.NewActualLRPKey("process-guid", int32(expectedIndex), "tests"),
					rep.NewResource(2048, 1024, 100),
					rep.NewPlacementConstraint(linuxRootFSURL, nil, []string{}),
				)

				task := rep.NewTask(
					"the-task-guid",
					"tests",
					rep.NewResource(2048, 1024, 100),
					rep.NewPlacementConstraint(linuxRootFSURL, nil, []string{}),
				)

				work = rep.Work{
					LRPs:  []rep.LRP{lrp},
					Tasks: []rep.Task{task},
				}
			})

			It("returns all work it was given", func() {
				Expect(cellRep.Perform(logger, work)).To(Equal(work))
			})
		})

		Describe("performing starts", func() {
			var (
				lrpAuctionOne,
				lrpAuctionTwo,
				lrpAuctionThree rep.LRP
				expectedGuidOne                = "instance-guid-1"
				expectedGuidTwo                = "instance-guid-2"
				expectedGuidThree              = "instance-guid-3"
				expectedIndexOne         int32 = 1
				expectedIndexTwo         int32 = 2
				expectedIndexThree       int32 = 3
				expectedIndexOneString         = "1"
				expectedIndexTwoString         = "2"
				expectedIndexThreeString       = "3"
				lrpAuctions              []rep.LRP
			)

			BeforeEach(func() {
				guidChan := make(chan string, 3)
				guidChan <- expectedGuidOne
				guidChan <- expectedGuidTwo
				guidChan <- expectedGuidThree

				fakeGenerateContainerGuid = func() (string, error) {
					return <-guidChan, nil
				}

				lrpAuctionOne = rep.NewLRP(
					"ig-1",
					models.NewActualLRPKey("process-guid", expectedIndexOne, "tests"),
					rep.NewResource(2048, 1024, 100),
					rep.NewPlacementConstraint("rootfs", []string{"pt-1"}, []string{"vd-1"}),
				)
				lrpAuctionTwo = rep.NewLRP(
					"ig-2",
					models.NewActualLRPKey("process-guid", expectedIndexTwo, "tests"),
					rep.NewResource(2048, 1024, 100),
					rep.NewPlacementConstraint("rootfs", []string{"pt-2"}, []string{}),
				)
				lrpAuctionThree = rep.NewLRP(
					"ig-3",
					models.NewActualLRPKey("process-guid", expectedIndexThree, "tests"),
					rep.NewResource(2048, 1024, 100),
					rep.NewPlacementConstraint("rootfs", []string{}, []string{"vd-3"}),
				)
			})

			JustBeforeEach(func() {
				lrpAuctions = []rep.LRP{lrpAuctionOne, lrpAuctionTwo, lrpAuctionThree}
			})

			Context("when all LRP Auctions can be successfully translated to container specs", func() {
				BeforeEach(func() {
					lrpAuctionOne.RootFs = linuxRootFSURL
					lrpAuctionTwo.RootFs = "unsupported-arbitrary://still-goes-through"
					lrpAuctionThree.RootFs = fmt.Sprintf("%s:linux?somekey=somevalue", models.PreloadedOCIRootFSScheme)

					proxyMemoryAllocation = 5
				})

				It("makes the correct allocation requests for all LRP Auctions", func() {
					_, err := cellRep.Perform(logger, rep.Work{
						LRPs: lrpAuctions,
					})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					_, arg := client.AllocateContainersArgsForCall(0)
					Expect(arg).To(ConsistOf(
						executor.AllocationRequest{
							Guid: rep.LRPContainerGuid(lrpAuctionOne.ProcessGuid, expectedGuidOne),
							Tags: executor.Tags{
								rep.LifecycleTag:     rep.LRPLifecycle,
								rep.DomainTag:        lrpAuctionOne.Domain,
								rep.ProcessGuidTag:   lrpAuctionOne.ProcessGuid,
								rep.InstanceGuidTag:  expectedGuidOne,
								rep.ProcessIndexTag:  expectedIndexOneString,
								rep.PlacementTagsTag: `["pt-1"]`,
								rep.VolumeDriversTag: `["vd-1"]`,
							},
							Resource: executor.NewResource(int(lrpAuctionOne.MemoryMB), int(lrpAuctionOne.DiskMB), int(lrpAuctionOne.MaxPids), linuxPath),
						},
						executor.AllocationRequest{
							Guid: rep.LRPContainerGuid(lrpAuctionTwo.ProcessGuid, expectedGuidTwo),
							Tags: executor.Tags{
								rep.LifecycleTag:     rep.LRPLifecycle,
								rep.DomainTag:        lrpAuctionTwo.Domain,
								rep.ProcessGuidTag:   lrpAuctionTwo.ProcessGuid,
								rep.InstanceGuidTag:  expectedGuidTwo,
								rep.ProcessIndexTag:  expectedIndexTwoString,
								rep.PlacementTagsTag: `["pt-2"]`,
								rep.VolumeDriversTag: `[]`,
							},
							Resource: executor.NewResource(int(lrpAuctionTwo.MemoryMB), int(lrpAuctionTwo.DiskMB), int(lrpAuctionTwo.MaxPids), "unsupported-arbitrary://still-goes-through"),
						},
						executor.AllocationRequest{
							Guid: rep.LRPContainerGuid(lrpAuctionThree.ProcessGuid, expectedGuidThree),
							Tags: executor.Tags{
								rep.LifecycleTag:     rep.LRPLifecycle,
								rep.DomainTag:        lrpAuctionThree.Domain,
								rep.ProcessGuidTag:   lrpAuctionThree.ProcessGuid,
								rep.InstanceGuidTag:  expectedGuidThree,
								rep.ProcessIndexTag:  expectedIndexThreeString,
								rep.PlacementTagsTag: `[]`,
								rep.VolumeDriversTag: `["vd-3"]`,
							},
							Resource: executor.NewResource(int(lrpAuctionThree.MemoryMB), int(lrpAuctionThree.DiskMB), int(lrpAuctionThree.MaxPids), fmt.Sprintf("%s:/data/rootfs/linux?somekey=somevalue", models.PreloadedOCIRootFSScheme)),
						},
					))
				})

				Context("when envoy needs to be placed in the container", func() {
					BeforeEach(func() {
						lrpAuctionOne.RootFs = linuxRootFSURL
						lrpAuctionTwo.RootFs = "unsupported-arbitrary://still-goes-through"
						lrpAuctionThree.RootFs = fmt.Sprintf("%s:linux?somekey=somevalue", models.PreloadedOCIRootFSScheme)

						enableContainerProxy = true
					})

					It("makes the correct allocation requests for all LRP Auctions with the additional memory allocation", func() {
						_, err := cellRep.Perform(logger, rep.Work{
							LRPs: lrpAuctions,
						})
						Expect(err).NotTo(HaveOccurred())

						Expect(client.AllocateContainersCallCount()).To(Equal(1))
						_, arg := client.AllocateContainersArgsForCall(0)
						Expect(arg).To(ConsistOf(
							executor.AllocationRequest{
								Guid: rep.LRPContainerGuid(lrpAuctionOne.ProcessGuid, expectedGuidOne),
								Tags: executor.Tags{
									rep.LifecycleTag:     rep.LRPLifecycle,
									rep.DomainTag:        lrpAuctionOne.Domain,
									rep.ProcessGuidTag:   lrpAuctionOne.ProcessGuid,
									rep.InstanceGuidTag:  expectedGuidOne,
									rep.ProcessIndexTag:  expectedIndexOneString,
									rep.PlacementTagsTag: `["pt-1"]`,
									rep.VolumeDriversTag: `["vd-1"]`,
								},
								Resource: executor.NewResource(int(lrpAuctionOne.MemoryMB+int32(proxyMemoryAllocation)), int(lrpAuctionOne.DiskMB), int(lrpAuctionOne.MaxPids), linuxPath),
							},
							executor.AllocationRequest{
								Guid: rep.LRPContainerGuid(lrpAuctionTwo.ProcessGuid, expectedGuidTwo),
								Tags: executor.Tags{
									rep.LifecycleTag:     rep.LRPLifecycle,
									rep.DomainTag:        lrpAuctionTwo.Domain,
									rep.ProcessGuidTag:   lrpAuctionTwo.ProcessGuid,
									rep.InstanceGuidTag:  expectedGuidTwo,
									rep.ProcessIndexTag:  expectedIndexTwoString,
									rep.PlacementTagsTag: `["pt-2"]`,
									rep.VolumeDriversTag: `[]`,
								},
								Resource: executor.NewResource(int(lrpAuctionTwo.MemoryMB), int(lrpAuctionTwo.DiskMB), int(lrpAuctionTwo.MaxPids), "unsupported-arbitrary://still-goes-through"),
							},
							executor.AllocationRequest{
								Guid: rep.LRPContainerGuid(lrpAuctionThree.ProcessGuid, expectedGuidThree),
								Tags: executor.Tags{
									rep.LifecycleTag:     rep.LRPLifecycle,
									rep.DomainTag:        lrpAuctionThree.Domain,
									rep.ProcessGuidTag:   lrpAuctionThree.ProcessGuid,
									rep.InstanceGuidTag:  expectedGuidThree,
									rep.ProcessIndexTag:  expectedIndexThreeString,
									rep.PlacementTagsTag: `[]`,
									rep.VolumeDriversTag: `["vd-3"]`,
								},
								Resource: executor.NewResource(int(lrpAuctionThree.MemoryMB+int32(proxyMemoryAllocation)), int(lrpAuctionThree.DiskMB), int(lrpAuctionThree.MaxPids), fmt.Sprintf("%s:/data/rootfs/linux?somekey=somevalue", models.PreloadedOCIRootFSScheme)),
							},
						))
					})
				})

				Context("when the workload's cell ID matches the cell's ID", func() {
					It("accepts the workload", func() {
						_, err := cellRep.Perform(logger, rep.Work{
							LRPs:   lrpAuctions,
							CellID: cellID,
						})
						Expect(err).NotTo(HaveOccurred())
					})
				})

				Context("when the workload's cell ID does not match the cell's ID", func() {
					It("rejects the workload", func() {
						_, err := cellRep.Perform(logger, rep.Work{
							LRPs:   lrpAuctions,
							CellID: "do-not-want-your-work",
						})
						Expect(err).To(MatchError(auctioncellrep.ErrCellIdMismatch))
					})
				})

				Context("when container proxy is enabled and there is not enough memory for the additional allocation", func() {
					BeforeEach(func() {
						enableContainerProxy = true
						lrpAuctions = append(lrpAuctions, lrpAuctionOne)
						client.RemainingResourcesReturns(executor.ExecutorResources{MemoryMB: 2048}, nil)
					})

					It("rejects the workload", func() {
						_, err := cellRep.Perform(logger, rep.Work{
							LRPs:   lrpAuctions,
							CellID: cellID,
						})
						Expect(err).To(MatchError(auctioncellrep.ErrNotEnoughMemory))
					})
				})

				Context("when all containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns([]executor.AllocationFailure{}, nil)
					})

					It("does not mark any LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(logger, rep.Work{LRPs: lrpAuctions})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork).To(BeZero())
					})
				})

				Context("when a container fails to be allocated", func() {
					BeforeEach(func() {
						resource := executor.NewResource(int(lrpAuctionOne.MemoryMB), int(lrpAuctionOne.DiskMB), int(lrpAuctionOne.MaxPids), "rootfs")
						tags := executor.Tags{}
						tags[rep.ProcessGuidTag] = lrpAuctionOne.ProcessGuid
						allocationRequest := executor.NewAllocationRequest(
							rep.LRPContainerGuid(lrpAuctionOne.ProcessGuid, expectedGuidOne),
							&resource,
							tags,
						)
						allocationFailure := executor.NewAllocationFailure(&allocationRequest, commonErr.Error())
						client.AllocateContainersReturns([]executor.AllocationFailure{allocationFailure}, nil)
					})

					It("marks the corresponding LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(logger, rep.Work{LRPs: lrpAuctions})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.LRPs).To(ConsistOf(lrpAuctionOne))
					})
				})
			})

			Context("when an LRP Auction specifies a preloaded RootFSes for which it cannot determine a RootFS path", func() {
				BeforeEach(func() {
					lrpAuctionOne.RootFs = linuxRootFSURL
					lrpAuctionTwo.RootFs = "preloaded:not-on-cell"
					lrpAuctionThree.RootFs = fmt.Sprintf("%s:not-on-cell?somekey=somevalue", models.PreloadedOCIRootFSScheme)
				})

				It("only makes container allocation requests for the remaining LRP Auctions", func() {
					_, err := cellRep.Perform(logger, rep.Work{LRPs: lrpAuctions})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					_, arg := client.AllocateContainersArgsForCall(0)
					Expect(arg).To(ConsistOf(
						executor.AllocationRequest{
							Guid: rep.LRPContainerGuid(lrpAuctionOne.ProcessGuid, expectedGuidOne),
							Tags: executor.Tags{
								rep.LifecycleTag:     rep.LRPLifecycle,
								rep.DomainTag:        lrpAuctionOne.Domain,
								rep.ProcessGuidTag:   lrpAuctionOne.ProcessGuid,
								rep.InstanceGuidTag:  expectedGuidOne,
								rep.ProcessIndexTag:  expectedIndexOneString,
								rep.PlacementTagsTag: `["pt-1"]`,
								rep.VolumeDriversTag: `["vd-1"]`,
							},
							Resource: executor.NewResource(int(lrpAuctionOne.MemoryMB), int(lrpAuctionOne.DiskMB), int(lrpAuctionOne.MaxPids), linuxPath),
						},
					))
				})

				It("marks the LRP Auction as failed", func() {
					failedWork, err := cellRep.Perform(logger, rep.Work{LRPs: lrpAuctions})
					Expect(err).NotTo(HaveOccurred())
					Expect(failedWork.LRPs).To(ConsistOf(lrpAuctionTwo, lrpAuctionThree))
				})

				Context("when a remaining container fails to be allocated", func() {
					BeforeEach(func() {
						resource := executor.NewResource(int(lrpAuctionOne.MemoryMB), int(lrpAuctionOne.DiskMB), int(lrpAuctionOne.MaxPids), "rootfs")
						tags := executor.Tags{}
						tags[rep.ProcessGuidTag] = lrpAuctionOne.ProcessGuid
						allocationRequest := executor.NewAllocationRequest(
							rep.LRPContainerGuid(lrpAuctionOne.ProcessGuid, expectedGuidOne),
							&resource,
							tags,
						)
						allocationFailure := executor.NewAllocationFailure(&allocationRequest, commonErr.Error())
						client.AllocateContainersReturns([]executor.AllocationFailure{allocationFailure}, nil)
					})

					It("marks the corresponding LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(logger, rep.Work{LRPs: lrpAuctions})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.LRPs).To(ConsistOf(lrpAuctionOne, lrpAuctionTwo, lrpAuctionThree))
					})
				})
			})

			Context("when an LRP Auction specifies a blank RootFS URL", func() {
				BeforeEach(func() {
					lrpAuctionOne.RootFs = ""
				})

				It("makes the correct allocation request for it, passing along the blank path to the executor client", func() {
					_, err := cellRep.Perform(logger, rep.Work{LRPs: []rep.LRP{lrpAuctionOne}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					_, arg := client.AllocateContainersArgsForCall(0)
					Expect(arg).To(ConsistOf(
						executor.AllocationRequest{
							Guid: rep.LRPContainerGuid(lrpAuctionOne.ProcessGuid, expectedGuidOne),
							Tags: executor.Tags{
								rep.LifecycleTag:     rep.LRPLifecycle,
								rep.DomainTag:        lrpAuctionOne.Domain,
								rep.ProcessGuidTag:   lrpAuctionOne.ProcessGuid,
								rep.InstanceGuidTag:  expectedGuidOne,
								rep.ProcessIndexTag:  expectedIndexOneString,
								rep.PlacementTagsTag: `["pt-1"]`,
								rep.VolumeDriversTag: `["vd-1"]`,
							},
							Resource: executor.NewResource(int(lrpAuctionOne.MemoryMB), int(lrpAuctionOne.DiskMB), int(lrpAuctionOne.MaxPids), ""),
						},
					))
				})
			})

			Context("when an LRP Auction specifies an invalid RootFS URL", func() {
				BeforeEach(func() {
					lrpAuctionOne.RootFs = linuxRootFSURL
					lrpAuctionTwo.RootFs = "%x"
				})

				It("only makes container allocation requests for the remaining LRP Auctions", func() {
					_, err := cellRep.Perform(logger, rep.Work{LRPs: []rep.LRP{lrpAuctionOne, lrpAuctionTwo}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					_, arg := client.AllocateContainersArgsForCall(0)
					Expect(arg).To(ConsistOf(
						executor.AllocationRequest{
							Guid: rep.LRPContainerGuid(lrpAuctionOne.ProcessGuid, expectedGuidOne),
							Tags: executor.Tags{
								rep.LifecycleTag:     rep.LRPLifecycle,
								rep.DomainTag:        lrpAuctionOne.Domain,
								rep.ProcessGuidTag:   lrpAuctionOne.ProcessGuid,
								rep.InstanceGuidTag:  expectedGuidOne,
								rep.ProcessIndexTag:  expectedIndexOneString,
								rep.PlacementTagsTag: `["pt-1"]`,
								rep.VolumeDriversTag: `["vd-1"]`,
							},
							Resource: executor.NewResource(int(lrpAuctionOne.MemoryMB), int(lrpAuctionOne.DiskMB), int(lrpAuctionOne.MaxPids), linuxPath),
						},
					))
				})

				It("marks the LRP Auction as failed", func() {
					failedWork, err := cellRep.Perform(logger, rep.Work{LRPs: lrpAuctions})
					Expect(err).NotTo(HaveOccurred())
					Expect(failedWork.LRPs).To(ContainElement(lrpAuctionTwo))
				})

				Context("when all remaining containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns([]executor.AllocationFailure{}, nil)
					})

					It("does not mark any additional LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(logger, rep.Work{LRPs: lrpAuctions})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.LRPs).To(ConsistOf(lrpAuctionTwo))
					})
				})

				Context("when a remaining container fails to be allocated", func() {
					BeforeEach(func() {
						resource := executor.NewResource(int(lrpAuctionOne.MemoryMB), int(lrpAuctionOne.DiskMB), int(lrpAuctionOne.MaxPids), "rootfs")
						tags := executor.Tags{}
						tags[rep.ProcessGuidTag] = lrpAuctionOne.ProcessGuid
						allocationRequest := executor.NewAllocationRequest(
							rep.LRPContainerGuid(lrpAuctionOne.ProcessGuid, expectedGuidOne),
							&resource,
							tags,
						)
						allocationFailure := executor.NewAllocationFailure(&allocationRequest, commonErr.Error())
						client.AllocateContainersReturns([]executor.AllocationFailure{allocationFailure}, nil)
					})

					It("marks the corresponding LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(logger, rep.Work{LRPs: lrpAuctions})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.LRPs).To(ConsistOf(lrpAuctionOne, lrpAuctionTwo))
					})
				})
			})
		})

		Describe("starting tasks", func() {
			var task1, task2 rep.Task

			BeforeEach(func() {
				resource1 := rep.NewResource(256, 512, 256)
				placement1 := rep.NewPlacementConstraint("tests", []string{"pt-1"}, []string{"vd-1"})
				task1 = rep.NewTask("the-task-guid-1", "tests", resource1, placement1)

				resource2 := rep.NewResource(512, 1024, 256)
				placement2 := rep.NewPlacementConstraint("linux", []string{"pt-2"}, []string{})
				task2 = rep.NewTask("the-task-guid-2", "tests", resource2, placement2)

				work = rep.Work{Tasks: []rep.Task{task}}
			})

			Context("when all Tasks can be successfully translated to container specs", func() {
				BeforeEach(func() {
					task1.RootFs = linuxRootFSURL
					task2.RootFs = "unsupported-arbitrary://still-goes-through"
				})

				It("makes the correct allocation requests for all Tasks", func() {
					_, err := cellRep.Perform(logger, rep.Work{Tasks: []rep.Task{task1, task2}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					_, arg := client.AllocateContainersArgsForCall(0)
					Expect(arg).To(ConsistOf(
						allocationRequestFromTask(task1, linuxPath, `["pt-1"]`, `["vd-1"]`),
						allocationRequestFromTask(task2, task2.RootFs, `["pt-2"]`, `[]`),
					))
				})

				Context("when all containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns([]executor.AllocationFailure{}, nil)
					})

					It("does not mark any Tasks as failed", func() {
						failedWork, err := cellRep.Perform(logger, rep.Work{Tasks: []rep.Task{task1, task2}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork).To(BeZero())
					})
				})

				Context("when a container fails to be allocated", func() {
					BeforeEach(func() {
						resource := executor.NewResource(int(task1.MemoryMB), int(task1.DiskMB), int(task1.MaxPids), "linux")
						tags := executor.Tags{}
						allocationRequest := executor.NewAllocationRequest(
							task1.TaskGuid,
							&resource,
							tags,
						)
						allocationFailure := executor.NewAllocationFailure(&allocationRequest, commonErr.Error())
						client.AllocateContainersReturns([]executor.AllocationFailure{allocationFailure}, nil)
					})

					It("marks the corresponding Tasks as failed", func() {
						failedWork, err := cellRep.Perform(logger, rep.Work{Tasks: []rep.Task{task1, task2}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.Tasks).To(ConsistOf(task1))
					})
				})
			})

			Context("when a Task specifies a preloaded RootFSes for which it cannot determine a RootFS path", func() {
				BeforeEach(func() {
					task1.RootFs = linuxRootFSURL
					task2.RootFs = "preloaded:not-on-cell"
				})

				It("only makes container allocation requests for the remaining Tasks", func() {
					_, err := cellRep.Perform(logger, rep.Work{Tasks: []rep.Task{task1, task2}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					_, arg := client.AllocateContainersArgsForCall(0)
					Expect(arg).To(ConsistOf(
						allocationRequestFromTask(task1, linuxPath, `["pt-1"]`, `["vd-1"]`),
					))
				})

				It("marks the Task as failed", func() {
					failedWork, err := cellRep.Perform(logger, rep.Work{Tasks: []rep.Task{task1, task2}})
					Expect(err).NotTo(HaveOccurred())
					Expect(failedWork.Tasks).To(ContainElement(task2))
				})

				Context("when all remaining containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns([]executor.AllocationFailure{}, nil)
					})

					It("does not mark any additional Tasks as failed", func() {
						failedWork, err := cellRep.Perform(logger, rep.Work{Tasks: []rep.Task{task1, task2}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.Tasks).To(ConsistOf(task2))
					})
				})

				Context("when a remaining container fails to be allocated", func() {
					BeforeEach(func() {
						request := allocationRequestFromTask(task1, linuxPath, `["pt-1"]`, `["vd-1"]`)
						allocationFailure := executor.NewAllocationFailure(&request, commonErr.Error())
						client.AllocateContainersReturns([]executor.AllocationFailure{allocationFailure}, nil)
					})

					It("marks the corresponding Tasks as failed", func() {
						failedWork, err := cellRep.Perform(logger, rep.Work{Tasks: []rep.Task{task1, task2}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.Tasks).To(ConsistOf(task1, task2))
					})
				})
			})

			Context("when a Task specifies a blank RootFS URL", func() {
				BeforeEach(func() {
					task1.RootFs = ""
				})

				It("makes the correct allocation request for it, passing along the blank path to the executor client", func() {
					_, err := cellRep.Perform(logger, rep.Work{Tasks: []rep.Task{task1}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					_, arg := client.AllocateContainersArgsForCall(0)
					Expect(arg).To(ConsistOf(
						allocationRequestFromTask(task1, "", `["pt-1"]`, `["vd-1"]`),
					))
				})
			})

			Context("when a Task specifies an invalid RootFS URL", func() {
				BeforeEach(func() {
					task1.RootFs = linuxRootFSURL
					task2.RootFs = "%x"
				})

				It("only makes container allocation requests for the remaining Tasks", func() {
					_, err := cellRep.Perform(logger, rep.Work{Tasks: []rep.Task{task1, task2}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					_, arg := client.AllocateContainersArgsForCall(0)
					Expect(arg).To(ConsistOf(
						allocationRequestFromTask(task1, linuxPath, `["pt-1"]`, `["vd-1"]`),
					))
				})

				It("marks the Task as failed", func() {
					failedWork, err := cellRep.Perform(logger, rep.Work{Tasks: []rep.Task{task1, task2}})
					Expect(err).NotTo(HaveOccurred())
					Expect(failedWork.Tasks).To(ContainElement(task2))
				})

				Context("when all remaining containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns([]executor.AllocationFailure{}, nil)
					})

					It("does not mark any additional LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(logger, rep.Work{Tasks: []rep.Task{task1, task2}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.Tasks).To(ConsistOf(task2))
					})
				})

				Context("when a remaining container fails to be allocated", func() {
					BeforeEach(func() {
						request := allocationRequestFromTask(task1, linuxPath, `["pt-1"]`, `["vd-1"]`)
						allocationFailure := executor.NewAllocationFailure(&request, commonErr.Error())
						client.AllocateContainersReturns([]executor.AllocationFailure{allocationFailure}, nil)
					})

					It("marks the corresponding Tasks as failed", func() {
						failedWork, err := cellRep.Perform(logger, rep.Work{Tasks: []rep.Task{task1, task2}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.Tasks).To(ConsistOf(task1, task2))
					})
				})
			})
		})
	})
})

func allocationRequestFromTask(task rep.Task, rootFSPath, placementTags, volumeDrivers string) executor.AllocationRequest {
	resource := executor.NewResource(int(task.MemoryMB), int(task.DiskMB), int(task.MaxPids), rootFSPath)
	return executor.NewAllocationRequest(
		task.TaskGuid,
		&resource,
		executor.Tags{
			rep.LifecycleTag:     rep.TaskLifecycle,
			rep.DomainTag:        task.Domain,
			rep.PlacementTagsTag: placementTags,
			rep.VolumeDriversTag: volumeDrivers,
		},
	)
}
