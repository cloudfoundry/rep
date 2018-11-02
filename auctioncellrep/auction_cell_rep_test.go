package auctioncellrep_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"code.cloudfoundry.org/bbs/models"
	"code.cloudfoundry.org/executor"
	"code.cloudfoundry.org/executor/containermetrics"
	fake_client "code.cloudfoundry.org/executor/fakes"
	"code.cloudfoundry.org/lager/lagertest"
	"code.cloudfoundry.org/rep"
	"code.cloudfoundry.org/rep/auctioncellrep"
	fakes "code.cloudfoundry.org/rep/auctioncellrep/auctioncellrepfakes"
	"code.cloudfoundry.org/rep/evacuation/evacuation_context/fake_evacuation_context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

const (
	repURL     = "https://foo.cell.service.cf.internal:8888"
	cellID     = "some-cell-id"
	linuxStack = "linux"
	linuxPath  = "/data/rootfs/linux"
)

var _ = Describe("AuctionCellRep", func() {
	var (
		cellRep                      *auctioncellrep.AuctionCellRep
		client                       *fake_client.FakeClient
		logger                       *lagertest.TestLogger
		evacuationReporter           *fake_evacuation_context.FakeEvacuationReporter
		fakeContainerMetricsProvider *fakes.FakeContainerMetricsProvider

		linuxRootFSURL string
		commonErr      error

		placementTags, optionalPlacementTags []string
		proxyMemoryAllocation                int
		enableContainerProxy                 bool

		fakeContainerAllocator *fakes.FakeBatchContainerAllocator
	)

	BeforeEach(func() {
		client = new(fake_client.FakeClient)
		logger = lagertest.NewTestLogger("test")
		evacuationReporter = &fake_evacuation_context.FakeEvacuationReporter{}
		fakeContainerMetricsProvider = new(fakes.FakeContainerMetricsProvider)
		fakeContainerAllocator = new(fakes.FakeBatchContainerAllocator)

		linuxRootFSURL = models.PreloadedRootFS(linuxStack)

		commonErr = errors.New("Failed to fetch")
		proxyMemoryAllocation = 12
		enableContainerProxy = false
		client.HealthyReturns(true)
	})

	JustBeforeEach(func() {
		cellRep = auctioncellrep.New(
			cellID,
			repURL,
			rep.StackPathMap{linuxStack: linuxPath},
			fakeContainerMetricsProvider,
			[]string{"docker"},
			"the-zone",
			client,
			evacuationReporter,
			placementTags,
			optionalPlacementTags,
			proxyMemoryAllocation,
			enableContainerProxy,
			fakeContainerAllocator,
		)
	})

	Describe("Metrics", func() {
		var (
			// 	containers []executor.Container
			metrics *rep.ContainerMetricsCollection
		)

		JustBeforeEach(func() {
			// client.ListContainersReturns(containers, nil)
			var err error
			metrics, err = cellRep.Metrics(logger)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when the rep has no containers", func() {
			It("should return the cell-id and empty lrp and task metrics", func() {
				Expect(metrics.CellID).To(Equal(cellID))
				Expect(metrics.LRPs).To(BeEmpty())
				Expect(metrics.Tasks).To(BeEmpty())
			})
		})

		Context("when the rep has an lrp container", func() {
			var metricValues containermetrics.CachedContainerMetrics
			BeforeEach(func() {
				metricValues = containermetrics.CachedContainerMetrics{
					MetricGUID:       "some-metric-guid",
					CPUUsageFraction: 0.8,
					DiskUsageBytes:   10,
					DiskQuotaBytes:   20,
					MemoryUsageBytes: 5,
					MemoryQuotaBytes: 10,
				}
				container := createContainer(executor.StateRunning, rep.LRPLifecycle)
				client.ListContainersReturns([]executor.Container{container}, nil)
				fakeContainerMetricsProvider.MetricsReturns(map[string]*containermetrics.CachedContainerMetrics{
					"some-container-guid": &metricValues,
				})
			})

			It("should return metrics for the running container", func() {
				Expect(metrics.LRPs).To(HaveLen(1))
				Expect(metrics.Tasks).To(BeEmpty())

				lrpMetrics := metrics.LRPs[0]
				Expect(lrpMetrics.ProcessGUID).To(Equal("some-process-guid"))
				Expect(lrpMetrics.InstanceGUID).To(Equal("some-instance-guid"))
				Expect(lrpMetrics.Index).To(Equal(int32(1)))
				Expect(lrpMetrics.CachedContainerMetrics).To(Equal(metricValues))
			})
		})

		Context("when the rep has a task container", func() {
			var metricValues containermetrics.CachedContainerMetrics
			BeforeEach(func() {
				metricValues = containermetrics.CachedContainerMetrics{
					MetricGUID:       "some-metric-guid",
					CPUUsageFraction: 0.8,
					DiskUsageBytes:   10,
					DiskQuotaBytes:   20,
					MemoryUsageBytes: 5,
					MemoryQuotaBytes: 10,
				}
				container := createContainer(executor.StateRunning, rep.TaskLifecycle)
				client.ListContainersReturns([]executor.Container{container}, nil)
				fakeContainerMetricsProvider.MetricsReturns(map[string]*containermetrics.CachedContainerMetrics{
					"some-container-guid": &metricValues,
				})
			})

			It("should return metrics for the running container", func() {
				Expect(metrics.LRPs).To(BeEmpty())
				Expect(metrics.Tasks).To(HaveLen(1))

				taskMetrics := metrics.Tasks[0]
				Expect(taskMetrics.TaskGUID).To(Equal("some-container-guid"))
				Expect(taskMetrics.CachedContainerMetrics).To(Equal(metricValues))
			})
		})
	})

	Describe("State", func() {
		var (
			containers []executor.Container
		)

		Context("when the rep has a container", func() {
			var (
				state rep.CellState
			)

			JustBeforeEach(func() {
				client.ListContainersReturns(containers, nil)
				var healthy bool
				var err error
				state, healthy, err = cellRep.State(logger)
				Expect(err).NotTo(HaveOccurred())
				Expect(healthy).To(BeTrue())
			})

			Context("with TaskLifecycle", func() {
				createTaskContainer := func(state executor.State) executor.Container {
					return createContainer(state, rep.TaskLifecycle)
				}

				Context("in Reserved state", func() {
					BeforeEach(func() {
						container := createTaskContainer(executor.StateReserved)
						containers = []executor.Container{container}
					})

					It("returns a running Task", func() {
						Expect(state.Tasks).To(HaveLen(1))
						Expect(state.Tasks[0].State).To(Equal(models.Task_Running))
					})
				})

				Context("in Running state", func() {
					BeforeEach(func() {
						container := createTaskContainer(executor.StateRunning)
						containers = []executor.Container{container}
					})

					It("returns a running Task", func() {
						Expect(state.Tasks).To(HaveLen(1))
						Expect(state.Tasks[0].State).To(Equal(models.Task_Running))
					})
				})

				Context("in Completed state", func() {
					BeforeEach(func() {
						container := createTaskContainer(executor.StateCompleted)
						containers = []executor.Container{container}
					})

					It("returns a completed Task", func() {
						Expect(state.Tasks).To(HaveLen(1))
						Expect(state.Tasks[0].State).To(Equal(models.Task_Completed))
					})

					Context("and the Failed flag is set", func() {
						BeforeEach(func() {
							containers[0].RunResult.Failed = true
						})

						It("returns a Failed Task", func() {
							Expect(state.Tasks).To(HaveLen(1))
							Expect(state.Tasks[0].State).To(Equal(models.Task_Completed))
							Expect(state.Tasks[0].Failed).To(BeTrue())
						})
					})
				})
			})

			Context("with LRPLifecycle", func() {
				createLRPContainer := func(state executor.State) executor.Container {
					return createContainer(state, rep.LRPLifecycle)
				}

				Context("in Reserved state", func() {
					BeforeEach(func() {
						containers = []executor.Container{createLRPContainer(executor.StateReserved)}
					})

					It("returns a claimed LRP", func() {
						Expect(state.LRPs).To(HaveLen(1))
						Expect(state.LRPs[0].State).To(Equal(models.ActualLRPStateClaimed))

						Expect(state.StartingContainerCount).To(Equal(1))
					})
				})

				Context("in Initializing state", func() {
					BeforeEach(func() {
						containers = []executor.Container{createLRPContainer(executor.StateInitializing)}
					})

					It("returns a claimed LRP", func() {
						Expect(state.LRPs).To(HaveLen(1))
						Expect(state.LRPs[0].State).To(Equal(models.ActualLRPStateClaimed))

						Expect(state.StartingContainerCount).To(Equal(1))
					})
				})

				Context("in Created state", func() {
					BeforeEach(func() {
						containers = []executor.Container{createLRPContainer(executor.StateCreated)}
					})

					It("returns a Claimed LRP", func() {
						Expect(state.LRPs).To(HaveLen(1))
						Expect(state.LRPs[0].State).To(Equal(models.ActualLRPStateClaimed))

						Expect(state.StartingContainerCount).To(Equal(1))
					})
				})

				Context("in Completed state", func() {
					BeforeEach(func() {
						containers = []executor.Container{createLRPContainer(executor.StateCompleted)}
					})

					It("returns a Running LRP", func() {
						Expect(state.LRPs).To(HaveLen(1))
						Expect(state.LRPs[0].State).To(Equal("SHUTDOWN"))

						Expect(state.StartingContainerCount).To(BeZero())
					})

					Context("and the LRP has crashed", func() {
						BeforeEach(func() {
							containers[0].RunResult.Failed = true
						})

						It("returns a Running LRP", func() {
							Expect(state.LRPs).To(HaveLen(1))
							Expect(state.LRPs[0].State).To(Equal("CRASHED"))

							Expect(state.StartingContainerCount).To(BeZero())
						})
					})
				})

				Context("in Running state", func() {
					BeforeEach(func() {
						containers = []executor.Container{createLRPContainer(executor.StateRunning)}
					})

					It("returns a Running LRP", func() {
						Expect(state.LRPs).To(HaveLen(1))
						Expect(state.LRPs[0].State).To(Equal(models.ActualLRPStateRunning))

						Expect(state.StartingContainerCount).To(BeZero())
					})

					Context("returns the right index", func() {
						BeforeEach(func() {
							containers[0].Tags[rep.ProcessIndexTag] = "100"
						})

						It("returns the right index", func() {
							Expect(state.LRPs).To(HaveLen(1))
							Expect(state.LRPs[0].Index).To(BeNumerically("==", 100))
						})
					})

					Context("with a different domain", func() {
						BeforeEach(func() {
							containers[0].Tags[rep.DomainTag] = "random-domain"
						})

						It("returns the right index", func() {
							Expect(state.LRPs).To(HaveLen(1))
							Expect(state.LRPs[0].Domain).To(Equal("random-domain"))
						})
					})

					Context("with a process guid", func() {
						BeforeEach(func() {
							containers[0].Tags[rep.ProcessGuidTag] = "random-guid"
						})

						It("returns the right index", func() {
							Expect(state.LRPs).To(HaveLen(1))
							Expect(state.LRPs[0].ProcessGuid).To(Equal("random-guid"))
						})
					})

					Context("with different rootfs", func() {
						BeforeEach(func() {
							containers[0].RootFSPath = "docker://cfdiegodocker/grace"
						})

						It("returns the right index", func() {
							Expect(state.LRPs).To(HaveLen(1))
							Expect(state.LRPs[0].RootFs).To(Equal("docker://cfdiegodocker/grace"))
						})
					})

					Context("with placement tags", func() {
						BeforeEach(func() {
							containers[0].Tags[rep.PlacementTagsTag] = `["random-placement-tag"]`
						})

						It("returns the right index", func() {
							Expect(state.LRPs).To(HaveLen(1))
							Expect(state.LRPs[0].PlacementTags).To(ConsistOf([]string{"random-placement-tag"}))
						})
					})

					Context("with volume drivers", func() {
						BeforeEach(func() {
							containers[0].Tags[rep.VolumeDriversTag] = `["random-volume-driver"]`
						})

						It("returns the right index", func() {
							Expect(state.LRPs).To(HaveLen(1))
							Expect(state.LRPs[0].VolumeDrivers).To(ConsistOf([]string{"random-volume-driver"}))
						})
					})

					Context("with different resource usage", func() {
						BeforeEach(func() {
							containers[0].Resource = executor.Resource{
								MemoryMB: 2048,
								DiskMB:   4096,
								MaxPids:  10,
							}
						})

						It("returns the right index", func() {
							Expect(state.LRPs).To(HaveLen(1))
							Expect(state.LRPs[0].Resource).To(Equal(rep.Resource{
								MemoryMB: 2048,
								DiskMB:   4096,
								MaxPids:  10,
							}))
						})
					})
				})
			})
		})

		It("queries the client and returns state", func() {
			evacuationReporter.EvacuatingReturns(true)
			totalResources := executor.ExecutorResources{
				MemoryMB:   1024,
				DiskMB:     2048,
				Containers: 4,
			}

			availableResources := executor.ExecutorResources{
				MemoryMB:   512,
				DiskMB:     256,
				Containers: 2,
			}

			volumeDrivers := []string{"lewis", "nico", "sebastian", "felipe"}

			client.TotalResourcesReturns(totalResources, nil)
			client.RemainingResourcesReturns(availableResources, nil)
			client.ListContainersReturns(containers, nil)
			client.VolumeDriversReturns(volumeDrivers, nil)

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

			Expect(state.VolumeDrivers).To(ConsistOf(volumeDrivers))

			Expect(state.ProxyMemoryAllocationMB).To(Equal(0))
		})

		Context("when enableContainerProxy is true", func() {
			BeforeEach(func() {
				enableContainerProxy = true
			})

			It("returns a state with a proxyMemoryAllocation greater than 0", func() {
				state, _, err := cellRep.State(logger)
				Expect(err).NotTo(HaveOccurred())

				Expect(state.ProxyMemoryAllocationMB).To(Equal(proxyMemoryAllocation))
			})
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
			remainingCellMemory int

			lrpAuctionOne, lrpAuctionTwo, lrpAuctionThree rep.LRP
			lrpAuctions                                   []rep.LRP
			work                                          rep.Work

			successfulLRP, unsuccessfulLRP   rep.LRP
			successfulTask, unsuccessfulTask rep.Task
		)

		BeforeEach(func() {
			remainingCellMemory = 8192

			successfulLRP = rep.NewLRP(
				"ig-1",
				models.ActualLRPKey{
					ProcessGuid: "process-guid",
					Index:       0,
					Domain:      "domain",
				},
				rep.Resource{},
				rep.PlacementConstraint{},
			)

			unsuccessfulLRP = rep.NewLRP(
				"ig-2",
				models.ActualLRPKey{
					ProcessGuid: "process-guid",
					Index:       1,
					Domain:      "domain",
				},
				rep.Resource{},
				rep.PlacementConstraint{},
			)

			successfulTask = rep.NewTask(
				"ig-1",
				"domain",
				rep.Resource{},
				rep.PlacementConstraint{},
			)

			unsuccessfulTask = rep.NewTask(
				"ig-2",
				"domain",
				rep.Resource{},
				rep.PlacementConstraint{},
			)

		})

		JustBeforeEach(func() {
			client.RemainingResourcesReturns(executor.ExecutorResources{MemoryMB: remainingCellMemory}, nil)
			lrpAuctions = []rep.LRP{lrpAuctionOne, lrpAuctionTwo, lrpAuctionThree}
		})

		It("requests container allocation for all provided LRPs and Tasks", func() {
			fakeContainerAllocator.BatchLRPAllocationRequestReturns([]rep.LRP{unsuccessfulLRP})
			fakeContainerAllocator.BatchTaskAllocationRequestReturns([]rep.Task{unsuccessfulTask})

			cellRep.Perform(logger, rep.Work{
				LRPs:  []rep.LRP{successfulLRP, unsuccessfulLRP},
				Tasks: []rep.Task{successfulTask, unsuccessfulTask},
			})

			Expect(fakeContainerAllocator.BatchLRPAllocationRequestCallCount()).To(Equal(1))
			_, lrpRequests := fakeContainerAllocator.BatchLRPAllocationRequestArgsForCall(0)
			Expect(lrpRequests).To(ConsistOf(successfulLRP, unsuccessfulLRP))

			Expect(fakeContainerAllocator.BatchTaskAllocationRequestCallCount()).To(Equal(1))
			_, taskRequests := fakeContainerAllocator.BatchTaskAllocationRequestArgsForCall(0)
			Expect(taskRequests).To(ConsistOf(successfulTask, unsuccessfulTask))
		})

		It("returns LRPs and Tasks that could not be allocated", func() {
			fakeContainerAllocator.BatchLRPAllocationRequestReturns([]rep.LRP{unsuccessfulLRP})
			fakeContainerAllocator.BatchTaskAllocationRequestReturns([]rep.Task{unsuccessfulTask})

			failedWork, err := cellRep.Perform(logger, rep.Work{
				LRPs:  []rep.LRP{successfulLRP, unsuccessfulLRP},
				Tasks: []rep.Task{successfulTask, unsuccessfulTask},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(failedWork.LRPs).To(ConsistOf(unsuccessfulLRP))
			Expect(failedWork.Tasks).To(ConsistOf(unsuccessfulTask))
		})

		Context("when evacuating", func() {
			BeforeEach(func() {
				evacuationReporter.EvacuatingReturns(true)

				lrp := rep.NewLRP(
					"ig-1",
					models.NewActualLRPKey("process-guid", 1, "tests"),
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

		Context("when the cell only has enough resources to run a subset of the workloads", func() {
			var smallestLRP, middleLRP, largestLRP rep.LRP

			BeforeEach(func() {
				remainingCellMemory = 8192
				largestLRP = rep.LRP{Resource: rep.Resource{MemoryMB: 6144}}
				middleLRP = rep.LRP{Resource: rep.Resource{MemoryMB: int32(remainingCellMemory) - largestLRP.MemoryMB}}
				smallestLRP = rep.LRP{Resource: rep.Resource{MemoryMB: 1}}
			})

			It("allocates containers for the largest workloads it can run", func() {
				failedWork, err := cellRep.Perform(logger, rep.Work{
					LRPs:  []rep.LRP{smallestLRP, middleLRP, largestLRP},
					Tasks: []rep.Task{},
				})

				Expect(err).NotTo(HaveOccurred())
				Expect(failedWork.LRPs).To(ConsistOf(smallestLRP))

				Expect(fakeContainerAllocator.BatchLRPAllocationRequestCallCount()).To(Equal(1))

				_, requestedLRPs := fakeContainerAllocator.BatchLRPAllocationRequestArgsForCall(0)
				Expect(requestedLRPs).To(ConsistOf(largestLRP, middleLRP))
			})

			Context("when envoy needs to be placed in the container", func() {
				BeforeEach(func() {
					enableContainerProxy = true
					proxyMemoryAllocation = remainingCellMemory - int(largestLRP.MemoryMB)
				})

				It("accounts for the proxy overhead when determining which workloads to run and which to reject", func() {
					failedWork, err := cellRep.Perform(logger, rep.Work{
						LRPs:  []rep.LRP{smallestLRP, middleLRP, largestLRP},
						Tasks: []rep.Task{},
					})

					Expect(err).NotTo(HaveOccurred())
					Expect(failedWork.LRPs).To(ConsistOf(smallestLRP, middleLRP))

					Expect(fakeContainerAllocator.BatchLRPAllocationRequestCallCount()).To(Equal(1))

					_, requestedLRPs := fakeContainerAllocator.BatchLRPAllocationRequestArgsForCall(0)
					Expect(requestedLRPs).To(ConsistOf(largestLRP))
				})
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
	})
})

var _ = Describe("ContainerAllocator", func() {
	var (
		proxyMemoryAllocation     int
		executorClient            *fake_client.FakeClient
		linuxRootFSURL            string
		fakeGenerateContainerGuid func() (string, error)
		logger                    *lagertest.TestLogger
		commonErr                 error

		allocator auctioncellrep.ContainerAllocator
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test")
		linuxRootFSURL = models.PreloadedRootFS(linuxStack)
		proxyMemoryAllocation = 0
		executorClient = new(fake_client.FakeClient)
		commonErr = errors.New("Failed to fetch")

		fakeGenerateContainerGuidCallCount := 0
		fakeGenerateContainerGuid = func() (string, error) {
			fakeGenerateContainerGuidCallCount += 1
			return fmt.Sprintf("ig-%d", fakeGenerateContainerGuidCallCount), nil
		}
	})

	JustBeforeEach(func() {
		allocator = auctioncellrep.NewContainerAllocator(
			fakeGenerateContainerGuid,
			rep.StackPathMap{linuxStack: linuxPath},
			proxyMemoryAllocation,
			executorClient,
		)
	})

	Describe("BatchLRPAllocationRequest", func() {
		var (
			lrp1, lrp2           rep.LRP
			lrpIndex1, lrpIndex2 int32
		)

		BeforeEach(func() {
			lrpIndex1 = 0
			lrpIndex2 = 1

			lrp1 = rep.NewLRP(
				"ig-1",
				models.NewActualLRPKey("process-guid", lrpIndex1, "tests"),
				rep.NewResource(2048, 1024, 100),
				rep.NewPlacementConstraint(linuxRootFSURL, []string{"pt-1"}, []string{"vd-1"}),
			)

			lrp2 = rep.NewLRP(
				"ig-2",
				models.NewActualLRPKey("process-guid", lrpIndex2, "tests"),
				rep.NewResource(2048, 1024, 100),
				rep.NewPlacementConstraint("rootfs", []string{"pt-2"}, []string{}),
			)
		})

		It("makes the correct allocation requests for all LRPs", func() {
			allocator.BatchLRPAllocationRequest(logger, []rep.LRP{lrp1, lrp2})

			Expect(executorClient.AllocateContainersCallCount()).To(Equal(1))
			_, arg := executorClient.AllocateContainersArgsForCall(0)
			Expect(arg).To(ConsistOf(
				allocationRequestFromLRP(lrp1, linuxPath),
				allocationRequestFromLRP(lrp2, lrp2.RootFs),
			))
		})

		It("does not mark any LRP Auctions as failed", func() {
			failedWork := allocator.BatchLRPAllocationRequest(logger, []rep.LRP{lrp1, lrp2})
			Expect(failedWork).To(BeEmpty())
		})

		Context("when a container fails to be allocated", func() {
			BeforeEach(func() {
				allocationRequest := allocationRequestFromLRP(lrp2, lrp2.RootFs)
				allocationFailure := executor.NewAllocationFailure(&allocationRequest, commonErr.Error())
				executorClient.AllocateContainersReturns([]executor.AllocationFailure{allocationFailure})
			})

			It("marks the corresponding LRP Auctions as failed", func() {
				failedWork := allocator.BatchLRPAllocationRequest(logger, []rep.LRP{lrp1, lrp2})
				Expect(failedWork).To(ConsistOf(lrp2))
			})
		})

		Context("when envoy needs to be placed in the container", func() {
			BeforeEach(func() {
				proxyMemoryAllocation = 32
			})

			It("makes the correct allocation requests for all LRP Auctions with the additional memory allocation", func() {
				allocator.BatchLRPAllocationRequest(logger, []rep.LRP{lrp1, lrp2})

				Expect(executorClient.AllocateContainersCallCount()).To(Equal(1))
				_, arg := executorClient.AllocateContainersArgsForCall(0)

				allocationRequest1 := allocationRequestFromLRP(lrp1, linuxPath)
				allocationRequest2 := allocationRequestFromLRP(lrp2, lrp2.RootFs)
				allocationRequest1.MemoryMB += proxyMemoryAllocation
				allocationRequest2.MemoryMB += proxyMemoryAllocation

				Expect(arg).To(ConsistOf(
					allocationRequest1,
					allocationRequest2,
				))
			})

			Context("when the LRP has unlimited memory and additional memory is allocated for the proxy", func() {
				BeforeEach(func() {
					lrp1.MemoryMB = 0
				})

				It("requests an LRP with unlimited memory", func() {
					allocator.BatchLRPAllocationRequest(logger, []rep.LRP{lrp1, lrp2})

					expectedResource := executor.NewResource(0, int(lrp1.DiskMB), int(lrp1.MaxPids), linuxPath)

					_, arg := executorClient.AllocateContainersArgsForCall(0)
					Expect(len(arg)).To(BeNumerically(">=", 1))

					Expect(arg[0].Resource).To(Equal(expectedResource))
				})
			})
		})

		Context("when no requests need to be made", func() {
			It("doesn't make any requests to the executorClient", func() {
				allocator.BatchLRPAllocationRequest(logger, []rep.LRP{})
				Expect(executorClient.AllocateContainersCallCount()).To(Equal(0))
			})
		})

		Describe("handling RootFS paths", func() {
			var validLRP, invalidLRP rep.LRP

			BeforeEach(func() {
				validLRP = rep.NewLRP(
					"ig-1",
					models.NewActualLRPKey("process-guid", lrpIndex1, "tests"),
					rep.NewResource(2048, 1024, 100),
					rep.NewPlacementConstraint(
						linuxRootFSURL,
						[]string{"pt-1"},
						[]string{"vd-1"},
					),
				)

				invalidLRP = rep.NewLRP(
					"ig-2",
					models.NewActualLRPKey("process-guid", lrpIndex2, "tests"),
					rep.NewResource(2048, 1024, 100),
					rep.NewPlacementConstraint("rootfs", []string{"pt-2"}, []string{}),
				)
			})

			Context("when an LRP specifies an invalid RootFS URL", func() {
				BeforeEach(func() {
					invalidLRP.RootFs = "%x"
				})

				It("only makes container allocation requests for the remaining LRPs", func() {
					allocator.BatchLRPAllocationRequest(logger, []rep.LRP{validLRP, invalidLRP})

					Expect(executorClient.AllocateContainersCallCount()).To(Equal(1))
					_, arg := executorClient.AllocateContainersArgsForCall(0)
					Expect(arg).To(ConsistOf(
						allocationRequestFromLRP(validLRP, linuxPath),
					))
				})

				It("marks the other LRP as failed", func() {
					failedLRPs := allocator.BatchLRPAllocationRequest(logger, []rep.LRP{validLRP, invalidLRP})
					Expect(failedLRPs).To(ConsistOf(invalidLRP))
				})
			})

			Context("when a LRP specifies a preloaded RootFSes for which it cannot determine a RootFS path", func() {
				BeforeEach(func() {
					invalidLRP.RootFs = "preloaded:not-on-cell"
				})

				It("only makes container allocation requests for the LRPs with valid RootFS paths", func() {
					allocator.BatchLRPAllocationRequest(logger, []rep.LRP{validLRP, invalidLRP})

					Expect(executorClient.AllocateContainersCallCount()).To(Equal(1))
					_, arg := executorClient.AllocateContainersArgsForCall(0)
					Expect(arg).To(ConsistOf(
						allocationRequestFromLRP(validLRP, linuxPath),
					))
				})

				It("marks the LRPs with invalid RootFS paths as failed", func() {
					failedLRPs := allocator.BatchLRPAllocationRequest(logger, []rep.LRP{validLRP, invalidLRP})
					Expect(failedLRPs).To(HaveLen(1))
					Expect(failedLRPs).To(ContainElement(invalidLRP))
				})
			})

			Context("when a LRP specifies a blank RootFS URL", func() {
				BeforeEach(func() {
					validLRP.RootFs = ""
				})

				It("makes the correct allocation request for it, passing along the blank path to the executor client", func() {
					allocator.BatchLRPAllocationRequest(logger, []rep.LRP{validLRP})

					Expect(executorClient.AllocateContainersCallCount()).To(Equal(1))
					_, arg := executorClient.AllocateContainersArgsForCall(0)
					Expect(arg).To(ConsistOf(
						allocationRequestFromLRP(validLRP, ""),
					))
				})
			})

			Context("when the lrp uses docker rootfs scheme", func() {
				BeforeEach(func() {
					validLRP.RootFs = "docker://cfdiegodocker/grace"
				})

				It("makes the container allocation request with an unchanged rootfs url", func() {
					allocator.BatchLRPAllocationRequest(logger, []rep.LRP{validLRP})

					Expect(executorClient.AllocateContainersCallCount()).To(Equal(1))
					_, arg := executorClient.AllocateContainersArgsForCall(0)
					Expect(arg).To(ConsistOf(
						allocationRequestFromLRP(validLRP, validLRP.RootFs),
					))
				})
			})
		})
	})

	Describe("BatchTaskAllocationRequest", func() {
		var (
			task1, task2 rep.Task
		)

		BeforeEach(func() {
			resource1 := rep.NewResource(256, 512, 256)
			placement1 := rep.NewPlacementConstraint("tests", []string{"pt-1"}, []string{"vd-1"})
			task1 = rep.NewTask("the-task-guid-1", "tests", resource1, placement1)
			task1.RootFs = linuxRootFSURL

			resource2 := rep.NewResource(512, 1024, 256)
			placement2 := rep.NewPlacementConstraint("linux", []string{"pt-2"}, []string{})
			task2 = rep.NewTask("the-task-guid-2", "tests", resource2, placement2)
			task2.RootFs = "unsupported-arbitrary://still-goes-through"
		})

		It("makes the correct allocation requests for all Tasks", func() {
			allocator.BatchTaskAllocationRequest(logger, []rep.Task{task1, task2})

			Expect(executorClient.AllocateContainersCallCount()).To(Equal(1))
			_, arg := executorClient.AllocateContainersArgsForCall(0)
			Expect(arg).To(ConsistOf(
				allocationRequestFromTask(task1, linuxPath, `["pt-1"]`, `["vd-1"]`),
				allocationRequestFromTask(task2, task2.RootFs, `["pt-2"]`, `[]`),
			))
		})

		Context("when all containers can be successfully allocated", func() {
			BeforeEach(func() {
				executorClient.AllocateContainersReturns([]executor.AllocationFailure{})
			})

			It("does not mark any Tasks as failed", func() {
				failedTasks := allocator.BatchTaskAllocationRequest(logger, []rep.Task{task1, task2})
				Expect(failedTasks).To(BeEmpty())
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
				executorClient.AllocateContainersReturns([]executor.AllocationFailure{allocationFailure})
			})

			It("marks the corresponding Tasks as failed", func() {
				failedTasks := allocator.BatchTaskAllocationRequest(logger, []rep.Task{task1, task2})
				Expect(failedTasks).To(ConsistOf(task1))
			})

			It("logs the container allocation failure", func() {
				allocator.BatchTaskAllocationRequest(logger, []rep.Task{task1, task2})
				Eventually(logger).Should(gbytes.Say("container-allocation-failure.*failed-request.*the-task-guid-1"))
			})
		})

		Context("when no requests need to be made", func() {
			It("doesn't make any requests to the executorClient", func() {
				allocator.BatchTaskAllocationRequest(logger, []rep.Task{})
				Expect(executorClient.AllocateContainersCallCount()).To(Equal(0))
			})
		})

		Describe("handling RootFS paths", func() {
			var validTask, invalidTask rep.Task

			BeforeEach(func() {
				resource1 := rep.NewResource(256, 512, 256)
				placement1 := rep.NewPlacementConstraint("tests", []string{"pt-1"}, []string{"vd-1"})
				validTask = rep.NewTask("the-task-guid-1", "tests", resource1, placement1)
				validTask.RootFs = linuxRootFSURL

				resource2 := rep.NewResource(512, 1024, 256)
				placement2 := rep.NewPlacementConstraint("linux", []string{"pt-2"}, []string{})
				invalidTask = rep.NewTask("the-task-guid-2", "tests", resource2, placement2)
			})

			Context("when a Task specifies an invalid RootFS URL", func() {
				BeforeEach(func() {
					invalidTask.RootFs = "%x"
				})

				It("only makes container allocation requests for the remaining Tasks", func() {
					allocator.BatchTaskAllocationRequest(logger, []rep.Task{validTask, invalidTask})

					Expect(executorClient.AllocateContainersCallCount()).To(Equal(1))
					_, arg := executorClient.AllocateContainersArgsForCall(0)
					Expect(arg).To(ConsistOf(
						allocationRequestFromTask(validTask, linuxPath, `["pt-1"]`, `["vd-1"]`),
					))
				})

				It("marks the Task as failed", func() {
					failedTasks := allocator.BatchTaskAllocationRequest(logger, []rep.Task{validTask, invalidTask})
					Expect(failedTasks).To(ConsistOf(invalidTask))
				})
			})

			Context("when a Task specifies a preloaded RootFSes for which it cannot determine a RootFS path", func() {
				BeforeEach(func() {
					invalidTask.RootFs = "preloaded:not-on-cell"
				})

				It("only makes container allocation requests for the tasks with valid RootFS paths", func() {
					allocator.BatchTaskAllocationRequest(logger, []rep.Task{validTask, invalidTask})

					Expect(executorClient.AllocateContainersCallCount()).To(Equal(1))
					_, arg := executorClient.AllocateContainersArgsForCall(0)
					Expect(arg).To(ConsistOf(
						allocationRequestFromTask(validTask, linuxPath, `["pt-1"]`, `["vd-1"]`),
					))
				})

				It("marks the tasks with invalid RootFS paths as failed", func() {
					failedTasks := allocator.BatchTaskAllocationRequest(logger, []rep.Task{validTask, invalidTask})
					Expect(failedTasks).To(HaveLen(1))
					Expect(failedTasks).To(ContainElement(invalidTask))
				})
			})

			Context("when a Task specifies a blank RootFS URL", func() {
				BeforeEach(func() {
					validTask.RootFs = ""
				})

				It("makes the correct allocation request for it, passing along the blank path to the executor client", func() {
					allocator.BatchTaskAllocationRequest(logger, []rep.Task{validTask})

					Expect(executorClient.AllocateContainersCallCount()).To(Equal(1))
					_, arg := executorClient.AllocateContainersArgsForCall(0)
					Expect(arg).To(ConsistOf(
						allocationRequestFromTask(validTask, "", `["pt-1"]`, `["vd-1"]`),
					))
				})
			})
		})
	})
})

func createContainer(state executor.State, lifecycle string) executor.Container {
	return executor.Container{
		Guid:     "some-container-guid",
		Resource: executor.NewResource(20, 10, 100, linuxPath),
		Tags: executor.Tags{
			rep.LifecycleTag:     lifecycle,
			rep.ProcessGuidTag:   "some-process-guid",
			rep.ProcessIndexTag:  "1",
			rep.DomainTag:        "domain",
			rep.InstanceGuidTag:  "some-instance-guid",
			rep.PlacementTagsTag: `["pt"]`,
			rep.VolumeDriversTag: `["vd"]`,
		},
		State: state,
	}
}

func allocationRequestFromLRP(lrp rep.LRP, rootFSPath string) executor.AllocationRequest {
	resource := executor.NewResource(
		int(lrp.MemoryMB),
		int(lrp.DiskMB),
		int(lrp.MaxPids),
		rootFSPath,
	)

	placementTagsBytes, err := json.Marshal(lrp.PlacementTags)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	volumeDriversBytes, err := json.Marshal(lrp.VolumeDrivers)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	return executor.NewAllocationRequest(
		lrp.InstanceGUID,
		&resource,
		executor.Tags{
			rep.LifecycleTag:     rep.LRPLifecycle,
			rep.DomainTag:        lrp.Domain,
			rep.PlacementTagsTag: string(placementTagsBytes),
			rep.VolumeDriversTag: string(volumeDriversBytes),
			rep.ProcessGuidTag:   lrp.ProcessGuid,
			rep.ProcessIndexTag:  strconv.Itoa(int(lrp.Index)),
			rep.InstanceGuidTag:  lrp.InstanceGUID,
		},
	)
}

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
