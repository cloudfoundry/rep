package auction_cell_rep_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/bbs/models"
	executor "github.com/cloudfoundry-incubator/executor"
	fake_client "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/auction_cell_rep"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context/fake_evacuation_context"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AuctionCellRep", func() {
	var cellRep rep.AuctionCellClient
	var client *fake_client.FakeClient
	var commonErr error
	var logger *lagertest.TestLogger
	var evacuationReporter *fake_evacuation_context.FakeEvacuationReporter

	const expectedCellID = "some-cell-id"
	var expectedGuid string
	var expectedGuidError error
	var fakeGenerateContainerGuid func() (string, error)

	const linuxStack = "linux"
	const linuxPath = "/data/rootfs/linux"
	var linuxRootFSURL string

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
		client.HealthyReturns(true)
	})

	JustBeforeEach(func() {
		cellRep = auction_cell_rep.New(expectedCellID, rep.StackPathMap{linuxStack: linuxPath}, []string{"docker"}, "the-zone", fakeGenerateContainerGuid, client, evacuationReporter, logger)
	})

	Describe("State", func() {
		var (
			availableResources, totalResources executor.ExecutorResources
			containers                         []executor.Container
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
					Resource: executor.NewResource(20, 10, "rootfs"),
					Tags: executor.Tags{
						rep.LifecycleTag:    rep.LRPLifecycle,
						rep.ProcessGuidTag:  "the-first-app-guid",
						rep.ProcessIndexTag: "17",
						rep.DomainTag:       "domain",
					},
				},
				{
					Guid:     "second",
					Resource: executor.NewResource(40, 30, "rootfs"),
					Tags: executor.Tags{
						rep.LifecycleTag:    rep.LRPLifecycle,
						rep.ProcessGuidTag:  "the-second-app-guid",
						rep.ProcessIndexTag: "92",
						rep.DomainTag:       "domain",
					},
				},
				{
					Guid:     "da-task",
					Resource: executor.NewResource(40, 30, "rootfs"),
					Tags: executor.Tags{
						rep.LifecycleTag: rep.TaskLifecycle,
						rep.DomainTag:    "domain",
					},
				},
				{
					Guid:     "other-task",
					Resource: executor.NewResource(40, 30, "rootfs"),
					Tags:     nil,
				},
			}

			client.TotalResourcesReturns(totalResources, nil)
			client.RemainingResourcesFromReturns(availableResources, nil)
			client.ListContainersReturns(containers, nil)
		})

		It("queries the client and returns state", func() {
			state, err := cellRep.State()
			Expect(err).NotTo(HaveOccurred())

			Expect(client.ListContainersArgsForCall(0)).To(Equal(executor.Tags{}))
			Expect(client.RemainingResourcesFromArgsForCall(0)).To(Equal(containers))

			Expect(state.Evacuating).To(BeTrue())
			Expect(state.RootFSProviders).To(Equal(rep.RootFSProviders{
				models.PreloadedRootFSScheme: rep.NewFixedSetRootFSProvider("linux"),
				"docker":                     rep.ArbitraryRootFSProvider{},
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
				rep.NewLRP(models.NewActualLRPKey("the-first-app-guid", 17, "domain"), rep.NewResource(20, 10, "")),
				rep.NewLRP(models.NewActualLRPKey("the-second-app-guid", 92, "domain"), rep.NewResource(40, 30, "")),
			}))

			Expect(state.Tasks).To(ConsistOf([]rep.Task{
				rep.NewTask("da-task", "domain", rep.NewResource(40, 30, "")),
			}))
		})

		Context("when the cell is not healthy", func() {
			BeforeEach(func() {
				client.HealthyReturns(false)
			})

			XIt("does ... something", func() {
				Fail("TODO")
			})
		})

		Context("when the client fails to fetch total resources", func() {
			BeforeEach(func() {
				client.TotalResourcesReturns(executor.ExecutorResources{}, commonErr)
			})

			It("should return an error and no state", func() {
				state, err := cellRep.State()
				Expect(state).To(BeZero())
				Expect(err).To(MatchError(commonErr))
			})
		})

		Context("when the client fails to fetch available resources", func() {
			BeforeEach(func() {
				client.RemainingResourcesFromReturns(executor.ExecutorResources{}, commonErr)
			})

			It("should return an error and no state", func() {
				state, err := cellRep.State()
				Expect(state).To(BeZero())
				Expect(err).To(MatchError(commonErr))
			})
		})

		Context("when the client fails to list containers", func() {
			BeforeEach(func() {
				client.ListContainersReturns(nil, commonErr)
			})

			It("should return an error and no state", func() {
				state, err := cellRep.State()
				Expect(state).To(BeZero())
				Expect(err).To(MatchError(commonErr))
			})
		})
	})

	Describe("Perform", func() {
		var (
			work rep.Work
			task rep.Task

			expectedIndex = 1
		)

		Context("when evacuating", func() {
			BeforeEach(func() {
				evacuationReporter.EvacuatingReturns(true)

				lrp := rep.NewLRP(
					models.NewActualLRPKey("process-guid", int32(expectedIndex), "tests"),
					rep.NewResource(2048, 1024, linuxRootFSURL),
				)
				task := rep.NewTask("the-task-guid", "tests", rep.NewResource(2048, 1024, linuxRootFSURL))

				work = rep.Work{
					LRPs:  []rep.LRP{lrp},
					Tasks: []rep.Task{task},
				}
			})

			It("returns all work it was given", func() {
				Expect(cellRep.Perform(work)).To(Equal(work))
			})
		})

		Describe("performing starts", func() {
			var lrpAuctionOne, lrpAuctionTwo rep.LRP
			var securityRule *models.SecurityGroupRule
			var expectedGuidOne = "instance-guid-1"
			var expectedGuidTwo = "instance-guid-2"
			var expectedIndexOne int32 = 1
			var expectedIndexTwo int32 = 2
			const expectedIndexOneString = "1"
			const expectedIndexTwoString = "2"

			BeforeEach(func() {
				guidChan := make(chan string, 2)
				guidChan <- expectedGuidOne
				guidChan <- expectedGuidTwo

				fakeGenerateContainerGuid = func() (string, error) {
					return <-guidChan, nil
				}

				securityRule = &models.SecurityGroupRule{
					Protocol:     "tcp",
					Destinations: []string{"0.0.0.0/0"},
					PortRange: &models.PortRange{
						Start: 1,
						End:   1024,
					},
				}

				lrpAuctionOne = rep.NewLRP(
					models.NewActualLRPKey("process-guid", expectedIndexOne, "tests"),
					rep.NewResource(2048, 1024, "rootfs"),
				)
				lrpAuctionTwo = rep.NewLRP(
					models.NewActualLRPKey("process-guid", expectedIndexTwo, "tests"),
					rep.NewResource(2048, 1024, "rootfs"),
				)
			})

			Context("when all LRP Auctions can be successfully translated to container specs", func() {
				BeforeEach(func() {
					lrpAuctionOne.RootFs = linuxRootFSURL
					lrpAuctionTwo.RootFs = "unsupported-arbitrary://still-goes-through"
				})

				It("makes the correct allocation requests for all LRP Auctions", func() {
					_, err := cellRep.Perform(rep.Work{
						LRPs: []rep.LRP{lrpAuctionOne, lrpAuctionTwo},
					})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					Expect(client.AllocateContainersArgsForCall(0)).To(ConsistOf(
						executor.AllocationRequest{
							Guid: rep.LRPContainerGuid(lrpAuctionOne.ProcessGuid, expectedGuidOne),
							Tags: executor.Tags{
								rep.LifecycleTag:    rep.LRPLifecycle,
								rep.DomainTag:       lrpAuctionOne.Domain,
								rep.ProcessGuidTag:  lrpAuctionOne.ProcessGuid,
								rep.InstanceGuidTag: expectedGuidOne,
								rep.ProcessIndexTag: expectedIndexOneString,
							},
							Resource: executor.NewResource(int(lrpAuctionOne.MemoryMB), int(lrpAuctionOne.DiskMB), linuxPath),
						},
						executor.AllocationRequest{
							Guid: rep.LRPContainerGuid(lrpAuctionTwo.ProcessGuid, expectedGuidTwo),
							Tags: executor.Tags{
								rep.LifecycleTag:    rep.LRPLifecycle,
								rep.DomainTag:       lrpAuctionTwo.Domain,
								rep.ProcessGuidTag:  lrpAuctionTwo.ProcessGuid,
								rep.InstanceGuidTag: expectedGuidTwo,
								rep.ProcessIndexTag: expectedIndexTwoString,
							},
							Resource: executor.NewResource(int(lrpAuctionTwo.MemoryMB), int(lrpAuctionTwo.DiskMB), "unsupported-arbitrary://still-goes-through"),
						},
					))
				})

				Context("when all containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns([]executor.AllocationFailure{}, nil)
					})

					It("does not mark any LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(rep.Work{LRPs: []rep.LRP{lrpAuctionOne, lrpAuctionTwo}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork).To(BeZero())
					})
				})

				Context("when a container fails to be allocated", func() {
					BeforeEach(func() {
						resource := executor.NewResource(int(lrpAuctionOne.MemoryMB), int(lrpAuctionOne.DiskMB), "rootfs")
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
						failedWork, err := cellRep.Perform(rep.Work{LRPs: []rep.LRP{lrpAuctionOne, lrpAuctionTwo}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.LRPs).To(ConsistOf(lrpAuctionOne))
					})
				})
			})

			Context("when an LRP Auction specifies a preloaded RootFSes for which it cannot determine a RootFS path", func() {
				BeforeEach(func() {
					lrpAuctionOne.RootFs = linuxRootFSURL
					lrpAuctionTwo.RootFs = "preloaded:not-on-cell"
				})

				It("only makes container allocation requests for the remaining LRP Auctions", func() {
					_, err := cellRep.Perform(rep.Work{LRPs: []rep.LRP{lrpAuctionOne, lrpAuctionTwo}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					Expect(client.AllocateContainersArgsForCall(0)).To(ConsistOf(
						executor.AllocationRequest{
							Guid: rep.LRPContainerGuid(lrpAuctionOne.ProcessGuid, expectedGuidOne),
							Tags: executor.Tags{
								rep.LifecycleTag:    rep.LRPLifecycle,
								rep.DomainTag:       lrpAuctionOne.Domain,
								rep.ProcessGuidTag:  lrpAuctionOne.ProcessGuid,
								rep.InstanceGuidTag: expectedGuidOne,
								rep.ProcessIndexTag: expectedIndexOneString,
							},
							Resource: executor.NewResource(int(lrpAuctionOne.MemoryMB), int(lrpAuctionOne.DiskMB), linuxPath),
						},
					))
				})

				It("marks the LRP Auction as failed", func() {
					failedWork, err := cellRep.Perform(rep.Work{LRPs: []rep.LRP{lrpAuctionOne, lrpAuctionTwo}})
					Expect(err).NotTo(HaveOccurred())
					Expect(failedWork.LRPs).To(ContainElement(lrpAuctionTwo))
				})

				Context("when all remaining containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns([]executor.AllocationFailure{}, nil)
					})

					It("does not mark any additional LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(rep.Work{LRPs: []rep.LRP{lrpAuctionOne, lrpAuctionTwo}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.LRPs).To(ConsistOf(lrpAuctionTwo))
					})
				})

				Context("when a remaining container fails to be allocated", func() {
					BeforeEach(func() {
						resource := executor.NewResource(int(lrpAuctionOne.MemoryMB), int(lrpAuctionOne.DiskMB), "rootfs")
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
						failedWork, err := cellRep.Perform(rep.Work{LRPs: []rep.LRP{lrpAuctionOne, lrpAuctionTwo}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.LRPs).To(ConsistOf(lrpAuctionOne, lrpAuctionTwo))
					})
				})
			})

			Context("when an LRP Auction specifies a blank RootFS URL", func() {
				BeforeEach(func() {
					lrpAuctionOne.RootFs = ""
				})

				It("makes the correct allocation request for it, passing along the blank path to the executor client", func() {
					_, err := cellRep.Perform(rep.Work{LRPs: []rep.LRP{lrpAuctionOne}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					Expect(client.AllocateContainersArgsForCall(0)).To(ConsistOf(
						executor.AllocationRequest{
							Guid: rep.LRPContainerGuid(lrpAuctionOne.ProcessGuid, expectedGuidOne),
							Tags: executor.Tags{
								rep.LifecycleTag:    rep.LRPLifecycle,
								rep.DomainTag:       lrpAuctionOne.Domain,
								rep.ProcessGuidTag:  lrpAuctionOne.ProcessGuid,
								rep.InstanceGuidTag: expectedGuidOne,
								rep.ProcessIndexTag: expectedIndexOneString,
							},
							Resource: executor.NewResource(int(lrpAuctionOne.MemoryMB), int(lrpAuctionOne.DiskMB), ""),
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
					_, err := cellRep.Perform(rep.Work{LRPs: []rep.LRP{lrpAuctionOne, lrpAuctionTwo}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					Expect(client.AllocateContainersArgsForCall(0)).To(ConsistOf(
						executor.AllocationRequest{
							Guid: rep.LRPContainerGuid(lrpAuctionOne.ProcessGuid, expectedGuidOne),
							Tags: executor.Tags{
								rep.LifecycleTag:    rep.LRPLifecycle,
								rep.DomainTag:       lrpAuctionOne.Domain,
								rep.ProcessGuidTag:  lrpAuctionOne.ProcessGuid,
								rep.InstanceGuidTag: expectedGuidOne,
								rep.ProcessIndexTag: expectedIndexOneString,
							},
							Resource: executor.NewResource(int(lrpAuctionOne.MemoryMB), int(lrpAuctionOne.DiskMB), linuxPath),
						},
					))
				})

				It("marks the LRP Auction as failed", func() {
					failedWork, err := cellRep.Perform(rep.Work{LRPs: []rep.LRP{lrpAuctionOne, lrpAuctionTwo}})
					Expect(err).NotTo(HaveOccurred())
					Expect(failedWork.LRPs).To(ContainElement(lrpAuctionTwo))
				})

				Context("when all remaining containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns([]executor.AllocationFailure{}, nil)
					})

					It("does not mark any additional LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(rep.Work{LRPs: []rep.LRP{lrpAuctionOne, lrpAuctionTwo}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.LRPs).To(ConsistOf(lrpAuctionTwo))
					})
				})

				Context("when a remaining container fails to be allocated", func() {
					BeforeEach(func() {
						resource := executor.NewResource(int(lrpAuctionOne.MemoryMB), int(lrpAuctionOne.DiskMB), "rootfs")
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
						failedWork, err := cellRep.Perform(rep.Work{LRPs: []rep.LRP{lrpAuctionOne, lrpAuctionTwo}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.LRPs).To(ConsistOf(lrpAuctionOne, lrpAuctionTwo))
					})
				})
			})
		})

		Describe("starting tasks", func() {
			var task1, task2 rep.Task

			BeforeEach(func() {
				resource1 := rep.NewResource(256, 512, "linux")
				task1 = rep.NewTask("the-task-guid-1", "tests", resource1)

				resource2 := rep.NewResource(512, 1024, "linux")
				task2 = rep.NewTask("the-task-guid-2", "tests", resource2)

				work = rep.Work{Tasks: []rep.Task{task}}
			})

			Context("when all Tasks can be successfully translated to container specs", func() {
				BeforeEach(func() {
					task1.RootFs = linuxRootFSURL
					task2.RootFs = "unsupported-arbitrary://still-goes-through"
				})

				It("makes the correct allocation requests for all Tasks", func() {
					_, err := cellRep.Perform(rep.Work{Tasks: []rep.Task{task1, task2}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					Expect(client.AllocateContainersArgsForCall(0)).To(ConsistOf(
						allocationRequestFromTask(task1, linuxPath),
						allocationRequestFromTask(task2, task2.RootFs),
					))
				})

				Context("when all containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns([]executor.AllocationFailure{}, nil)
					})

					It("does not mark any Tasks as failed", func() {
						failedWork, err := cellRep.Perform(rep.Work{Tasks: []rep.Task{task1, task2}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork).To(BeZero())
					})
				})

				Context("when a container fails to be allocated", func() {
					BeforeEach(func() {
						resource := executor.NewResource(int(task1.MemoryMB), int(task1.DiskMB), "linux")
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
						failedWork, err := cellRep.Perform(rep.Work{Tasks: []rep.Task{task1, task2}})
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
					_, err := cellRep.Perform(rep.Work{Tasks: []rep.Task{task1, task2}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					Expect(client.AllocateContainersArgsForCall(0)).To(ConsistOf(
						allocationRequestFromTask(task1, linuxPath),
					))
				})

				It("marks the Task as failed", func() {
					failedWork, err := cellRep.Perform(rep.Work{Tasks: []rep.Task{task1, task2}})
					Expect(err).NotTo(HaveOccurred())
					Expect(failedWork.Tasks).To(ContainElement(task2))
				})

				Context("when all remaining containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns([]executor.AllocationFailure{}, nil)
					})

					It("does not mark any additional Tasks as failed", func() {
						failedWork, err := cellRep.Perform(rep.Work{Tasks: []rep.Task{task1, task2}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.Tasks).To(ConsistOf(task2))
					})
				})

				Context("when a remaining container fails to be allocated", func() {
					BeforeEach(func() {
						request := allocationRequestFromTask(task1, linuxPath)
						allocationFailure := executor.NewAllocationFailure(&request, commonErr.Error())
						client.AllocateContainersReturns([]executor.AllocationFailure{allocationFailure}, nil)
					})

					It("marks the corresponding Tasks as failed", func() {
						failedWork, err := cellRep.Perform(rep.Work{Tasks: []rep.Task{task1, task2}})
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
					_, err := cellRep.Perform(rep.Work{Tasks: []rep.Task{task1}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					Expect(client.AllocateContainersArgsForCall(0)).To(ConsistOf(
						allocationRequestFromTask(task1, ""),
					))
				})
			})

			Context("when a Task specifies an invalid RootFS URL", func() {
				BeforeEach(func() {
					task1.RootFs = linuxRootFSURL
					task2.RootFs = "%x"
				})

				It("only makes container allocation requests for the remaining Tasks", func() {
					_, err := cellRep.Perform(rep.Work{Tasks: []rep.Task{task1, task2}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					Expect(client.AllocateContainersArgsForCall(0)).To(ConsistOf(
						allocationRequestFromTask(task1, linuxPath),
					))
				})

				It("marks the Task as failed", func() {
					failedWork, err := cellRep.Perform(rep.Work{Tasks: []rep.Task{task1, task2}})
					Expect(err).NotTo(HaveOccurred())
					Expect(failedWork.Tasks).To(ContainElement(task2))
				})

				Context("when all remaining containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns([]executor.AllocationFailure{}, nil)
					})

					It("does not mark any additional LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(rep.Work{Tasks: []rep.Task{task1, task2}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.Tasks).To(ConsistOf(task2))
					})
				})

				Context("when a remaining container fails to be allocated", func() {
					BeforeEach(func() {
						request := allocationRequestFromTask(task1, linuxPath)
						allocationFailure := executor.NewAllocationFailure(&request, commonErr.Error())
						client.AllocateContainersReturns([]executor.AllocationFailure{allocationFailure}, nil)
					})

					It("marks the corresponding Tasks as failed", func() {
						failedWork, err := cellRep.Perform(rep.Work{Tasks: []rep.Task{task1, task2}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.Tasks).To(ConsistOf(task1, task2))
					})
				})
			})
		})
	})
})

func allocationRequestFromTask(task rep.Task, rootFSPath string) executor.AllocationRequest {
	resource := executor.NewResource(int(task.MemoryMB), int(task.DiskMB), rootFSPath)
	return executor.NewAllocationRequest(
		task.TaskGuid,
		&resource,
		executor.Tags{
			rep.LifecycleTag: rep.TaskLifecycle,
			rep.DomainTag:    task.Domain,
		},
	)
}
