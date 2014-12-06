package auction_cell_rep_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	executor "github.com/cloudfoundry-incubator/executor"
	fake_client "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	. "github.com/cloudfoundry-incubator/rep/auction_cell_rep"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper/fake_lrp_stopper"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AuctionCellRep", func() {
	var cellRep auctiontypes.CellRep
	var client *fake_client.FakeClient
	var commonErr error
	var bbs *fake_bbs.FakeRepBBS
	var stopper *fake_lrp_stopper.FakeLRPStopper

	BeforeEach(func() {
		stopper = &fake_lrp_stopper.FakeLRPStopper{}
		client = new(fake_client.FakeClient)
		bbs = &fake_bbs.FakeRepBBS{}
		cellRep = New("some-cell-id", "lucid64", stopper, bbs, client, lagertest.NewTestLogger("test"))
		commonErr = errors.New("Failed to fetch")
	})

	Describe("State", func() {
		var availableResources, totalResources executor.ExecutorResources
		var containers []executor.Container
		BeforeEach(func() {
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
					DiskMB:   10,
					MemoryMB: 20,
					Tags: executor.Tags{
						rep.LifecycleTag:    rep.LRPLifecycle,
						rep.ProcessGuidTag:  "the-first-app-guid",
						rep.ProcessIndexTag: "17",
					},
				},
				{
					Guid:     "second",
					DiskMB:   30,
					MemoryMB: 40,
					Tags: executor.Tags{
						rep.LifecycleTag:    rep.LRPLifecycle,
						rep.ProcessGuidTag:  "the-second-app-guid",
						rep.ProcessIndexTag: "92",
					},
				},
			}

			client.TotalResourcesReturns(totalResources, nil)
			client.RemainingResourcesReturns(availableResources, nil)
			client.ListContainersReturns(containers, nil)
		})

		It("queries the client and returns state", func() {
			state, err := cellRep.State()
			Ω(err).ShouldNot(HaveOccurred())

			Ω(client.ListContainersArgsForCall(0)).Should(Equal(executor.Tags{
				rep.LifecycleTag: rep.LRPLifecycle,
			}))

			Ω(state.Stack).Should(Equal("lucid64"))
			Ω(state.AvailableResources).Should(Equal(auctiontypes.Resources{
				MemoryMB:   availableResources.MemoryMB,
				DiskMB:     availableResources.DiskMB,
				Containers: availableResources.Containers,
			}))
			Ω(state.TotalResources).Should(Equal(auctiontypes.Resources{
				MemoryMB:   totalResources.MemoryMB,
				DiskMB:     totalResources.DiskMB,
				Containers: totalResources.Containers,
			}))
			Ω(state.LRPs).Should(ConsistOf([]auctiontypes.LRP{
				{
					ProcessGuid:  "the-first-app-guid",
					Index:        17,
					InstanceGuid: "first",
					DiskMB:       10,
					MemoryMB:     20,
				},
				{
					ProcessGuid:  "the-second-app-guid",
					Index:        92,
					InstanceGuid: "second",
					DiskMB:       30,
					MemoryMB:     40,
				},
			}))
		})

		Context("when the client fails to fetch total resources", func() {
			BeforeEach(func() {
				client.TotalResourcesReturns(executor.ExecutorResources{}, commonErr)
			})

			It("should return an error and no state", func() {
				state, err := cellRep.State()
				Ω(state).Should(BeZero())
				Ω(err).Should(MatchError(commonErr))
			})
		})

		Context("when the client fails to fetch available resources", func() {
			BeforeEach(func() {
				client.RemainingResourcesReturns(executor.ExecutorResources{}, commonErr)
			})

			It("should return an error and no state", func() {
				state, err := cellRep.State()
				Ω(state).Should(BeZero())
				Ω(err).Should(MatchError(commonErr))
			})
		})

		Context("when the client fails to list containers", func() {
			BeforeEach(func() {
				client.ListContainersReturns(nil, commonErr)
			})

			It("should return an error and no state", func() {
				state, err := cellRep.State()
				Ω(state).Should(BeZero())
				Ω(err).Should(MatchError(commonErr))
			})
		})
	})

	Describe("performing work", func() {
		var work auctiontypes.Work
		var startAuction models.LRPStartAuction
		var startingLRP models.ActualLRP
		var stoppingLRP models.ActualLRP
		BeforeEach(func() {
			work = auctiontypes.Work{}

			startAuction = models.LRPStartAuction{
				DesiredLRP: models.DesiredLRP{
					Domain:      "tests",
					RootFSPath:  "some-root-fs",
					ProcessGuid: "process-guid",
					DiskMB:      1024,
					MemoryMB:    2048,
					CPUWeight:   42,
					EnvironmentVariables: []models.EnvironmentVariable{
						{Name: "var1", Value: "val1"},
						{Name: "var2", Value: "val2"},
					},
					Action: &models.DownloadAction{
						From: "http://example.com/something",
						To:   "/something",
					},
					LogGuid: "log-guid",
					Ports: []uint32{
						8080,
					},
				},

				InstanceGuid: "instance-guid",
				Index:        2,
			}

			startingLRP = models.NewActualLRP(startAuction.DesiredLRP.ProcessGuid,
				startAuction.InstanceGuid, "some-cell-id",
				startAuction.DesiredLRP.Domain, startAuction.Index, "")

			stoppingLRP = models.ActualLRP{
				ProcessGuid:  "some-process-guid",
				InstanceGuid: "some-instance-guid",
				Index:        2,

				CellID: "some-cell-id",
			}

			work.Starts = []models.LRPStartAuction{startAuction}

			work.Stops = []models.ActualLRP{stoppingLRP}
			work.Stops = []models.ActualLRP{stoppingLRP}
		})

		Describe("performing starts", func() {
			Context("when all is well", func() {
				It("should allocate a container, mark the lrp as started, and run it", func() {
					failedWork, err := cellRep.Perform(work)
					Ω(err).ShouldNot(HaveOccurred())
					Ω(failedWork).Should(BeZero())

					By("allocating the container")
					Ω(client.AllocateContainerCallCount()).Should(Equal(1))

					two := 2
					Ω(client.AllocateContainerArgsForCall(0)).Should(Equal(executor.Container{
						Guid: startAuction.InstanceGuid,

						Tags: executor.Tags{
							rep.LifecycleTag:    rep.LRPLifecycle,
							rep.DomainTag:       "tests",
							rep.ProcessGuidTag:  startAuction.DesiredLRP.ProcessGuid,
							rep.ProcessIndexTag: "2",
						},

						MemoryMB:   startAuction.DesiredLRP.MemoryMB,
						DiskMB:     startAuction.DesiredLRP.DiskMB,
						CPUWeight:  startAuction.DesiredLRP.CPUWeight,
						RootFSPath: "some-root-fs",
						Ports:      []executor.PortMapping{{ContainerPort: 8080}},
						Log:        executor.LogConfig{Guid: "log-guid", Index: &two},

						Setup:   startAuction.DesiredLRP.Setup,
						Action:  startAuction.DesiredLRP.Action,
						Monitor: startAuction.DesiredLRP.Monitor,

						Env: []executor.EnvironmentVariable{
							{Name: "INSTANCE_GUID", Value: "instance-guid"},
							{Name: "INSTANCE_INDEX", Value: "2"},
							{Name: "var1", Value: "val1"},
							{Name: "var2", Value: "val2"},
						},
					}))

					By("reporting the LRP as started")
					Ω(bbs.ClaimActualLRPCallCount()).Should(Equal(1))

					claimingLRP := bbs.ClaimActualLRPArgsForCall(0)
					Ω(claimingLRP).Should(Equal(startingLRP))

					By("running the LRP")
					Ω(client.RunContainerCallCount()).Should(Equal(1))
					Ω(client.RunContainerArgsForCall(0)).Should(Equal(startAuction.InstanceGuid))
				})
			})

			Context("when the container fails to allocate", func() {
				BeforeEach(func() {
					client.AllocateContainerReturns(executor.Container{}, commonErr)
				})

				It("should mark the start as failed", func() {
					failedWork, err := cellRep.Perform(work)
					Ω(err).ShouldNot(HaveOccurred(), "note: we don't error")
					Ω(failedWork.Starts).Should(ConsistOf(startAuction))
				})

				It("should not report to the BBS, or try to run the container", func() {
					cellRep.Perform(work)
					Ω(bbs.ClaimActualLRPCallCount()).Should(Equal(0))
					Ω(client.RunContainerCallCount()).Should(Equal(0))
				})
			})

			Context("when it fails to report to the BBS", func() {
				BeforeEach(func() {
					bbs.ClaimActualLRPReturns(nil, commonErr)
				})

				It("should mark the start as failed", func() {
					failedWork, err := cellRep.Perform(work)
					Ω(err).ShouldNot(HaveOccurred(), "note: we don't error")
					Ω(failedWork.Starts).Should(ConsistOf(startAuction))
				})

				It("should delete the container and not try to run the container", func() {
					cellRep.Perform(work)
					Ω(client.DeleteContainerCallCount()).Should(Equal(1))
					Ω(client.DeleteContainerArgsForCall(0)).Should(Equal(startAuction.InstanceGuid))
					Ω(client.RunContainerCallCount()).Should(Equal(0))
				})
			})

			Context("when it fails to run the container", func() {
				BeforeEach(func() {
					client.RunContainerReturns(commonErr)
				})

				It("should mark the start as failed", func() {
					failedWork, err := cellRep.Perform(work)
					Ω(err).ShouldNot(HaveOccurred(), "note: we don't error")
					Ω(failedWork.Starts).Should(ConsistOf(startAuction))
				})

				It("should delete the container and remove the Actual from the BBS", func() {
					cellRep.Perform(work)
					Ω(client.DeleteContainerCallCount()).Should(Equal(1))
					Ω(client.DeleteContainerArgsForCall(0)).Should(Equal(startAuction.InstanceGuid))
					Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(1))
				})
			})
		})

		Describe("performing stops", func() {
			Context("when all is well", func() {
				It("should instruct the LRPStopper to stop", func() {
					failedWork, err := cellRep.Perform(work)
					Ω(err).ShouldNot(HaveOccurred())
					Ω(failedWork).Should(BeZero())
					Ω(stopper.StopInstanceArgsForCall(0)).Should(Equal(stoppingLRP))
					Ω(stopper.StopInstanceArgsForCall(0)).Should(Equal(stoppingLRP))
				})
			})

			Context("when the stop fails", func() {
				BeforeEach(func() {
					stopper.StopInstanceReturns(commonErr)
				})

				It("should mark the stop as failed", func() {
					failedWork, err := cellRep.Perform(work)
					Ω(err).ShouldNot(HaveOccurred(), "note: we don't error")
					Ω(failedWork.Stops).Should(ConsistOf(stoppingLRP))
					Ω(failedWork.Stops).Should(ConsistOf(stoppingLRP))
				})
			})
		})
	})
})
