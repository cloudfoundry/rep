package auction_cell_rep_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/bbs/models/internal/model_helpers"
	executor "github.com/cloudfoundry-incubator/executor"
	fake_client "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/auction_cell_rep"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context/fake_evacuation_context"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AuctionCellRep", func() {
	var cellRep auctiontypes.CellRep
	var client *fake_client.FakeClient
	var commonErr error
	var bbs *fake_bbs.FakeRepBBS
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
		bbs = &fake_bbs.FakeRepBBS{}
		logger = lagertest.NewTestLogger("test")
		evacuationReporter = &fake_evacuation_context.FakeEvacuationReporter{}

		expectedGuid = "container-guid"
		expectedGuidError = nil
		fakeGenerateContainerGuid = func() (string, error) {
			return expectedGuid, expectedGuidError
		}
		linuxRootFSURL = models.PreloadedRootFS(linuxStack)

		commonErr = errors.New("Failed to fetch")
	})

	JustBeforeEach(func() {
		cellRep = auction_cell_rep.New(expectedCellID, rep.StackPathMap{linuxStack: linuxPath}, []string{"docker"}, "the-zone", fakeGenerateContainerGuid, bbs, client, evacuationReporter, logger)
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
			Expect(err).NotTo(HaveOccurred())

			Expect(client.ListContainersArgsForCall(0)).To(Equal(executor.Tags{
				rep.LifecycleTag: rep.LRPLifecycle,
			}))

			Expect(state.Evacuating).To(BeTrue())
			Expect(state.RootFSProviders).To(Equal(auctiontypes.RootFSProviders{
				models.PreloadedRootFSScheme: auctiontypes.NewFixedSetRootFSProvider("linux"),
				"docker":                     auctiontypes.ArbitraryRootFSProvider{},
			}))

			Expect(state.AvailableResources).To(Equal(auctiontypes.Resources{
				MemoryMB:   availableResources.MemoryMB,
				DiskMB:     availableResources.DiskMB,
				Containers: availableResources.Containers,
			}))

			Expect(state.TotalResources).To(Equal(auctiontypes.Resources{
				MemoryMB:   totalResources.MemoryMB,
				DiskMB:     totalResources.DiskMB,
				Containers: totalResources.Containers,
			}))

			Expect(state.LRPs).To(ConsistOf([]auctiontypes.LRP{
				{
					ProcessGuid: "the-first-app-guid",
					Index:       17,
					DiskMB:      10,
					MemoryMB:    20,
				},
				{
					ProcessGuid: "the-second-app-guid",
					Index:       92,
					DiskMB:      30,
					MemoryMB:    40,
				},
			}))

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
				client.RemainingResourcesReturns(executor.ExecutorResources{}, commonErr)
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
			work       auctiontypes.Work
			lrpAuction auctiontypes.LRPAuction
			task       *models.Task

			expectedIndex = 1
		)

		Context("when evacuating", func() {
			BeforeEach(func() {
				evacuationReporter.EvacuatingReturns(true)

				lrpAuction = auctiontypes.LRPAuction{
					DesiredLRP: &models.DesiredLRP{
						Domain:      "tests",
						RootFs:      linuxRootFSURL,
						ProcessGuid: "process-guid",
						DiskMb:      1024,
						MemoryMb:    2048,
						CpuWeight:   42,
						Privileged:  true,
						LogGuid:     "log-guid",
					},

					Index: expectedIndex,
				}

				task = model_helpers.NewValidTask("the-task-guid")
				task.Domain = "tests"
				task.RootFs = linuxRootFSURL
				//				task.DiskMb = 1024
				//				task.MemoryMb = 2048
				//				task.Privileged = true
				//				task.CpuWeight = 10

				work = auctiontypes.Work{
					LRPs:  []auctiontypes.LRPAuction{lrpAuction},
					Tasks: []*models.Task{task},
				}
			})

			It("returns all work it was given", func() {
				Expect(cellRep.Perform(work)).To(Equal(work))
			})
		})

		Describe("performing starts", func() {
			var lrpAuctionOne auctiontypes.LRPAuction
			var lrpAuctionTwo auctiontypes.LRPAuction
			var securityRule *models.SecurityGroupRule
			var expectedGuidOne = "instance-guid-1"
			var expectedGuidTwo = "instance-guid-2"
			var expectedIndexOne = 1
			var expectedIndexTwo = 2
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
				lrpAuctionOne = auctiontypes.LRPAuction{
					DesiredLRP: &models.DesiredLRP{
						Domain:      "tests",
						ProcessGuid: "process-guid",
						DiskMb:      1024,
						MemoryMb:    2048,
						CpuWeight:   42,
						Privileged:  true,
						EnvironmentVariables: []*models.EnvironmentVariable{
							{Name: "var1", Value: "val1"},
							{Name: "var2", Value: "val2"},
						},
						Action: models.WrapAction(&models.DownloadAction{
							From: "http://example.com/something",
							To:   "/something",
						}),
						LogGuid:     "log-guid",
						LogSource:   "log-source",
						MetricsGuid: "metrics-guid",
						Ports: []uint32{
							8080,
						},
						EgressRules: []*models.SecurityGroupRule{
							securityRule,
						},
					},

					Index: expectedIndexOne,
				}

				lrpAuctionTwo = auctiontypes.LRPAuction{
					DesiredLRP: &models.DesiredLRP{
						Domain:      "tests",
						ProcessGuid: "process-guid",
						DiskMb:      1024,
						MemoryMb:    2048,
						CpuWeight:   42,
						Privileged:  true,
						EnvironmentVariables: []*models.EnvironmentVariable{
							{Name: "var1", Value: "val1"},
							{Name: "var2", Value: "val2"},
						},
						Action: models.WrapAction(&models.DownloadAction{
							From: "http://example.com/something",
							To:   "/something",
						}),
						LogGuid:     "log-guid",
						LogSource:   "log-source",
						MetricsGuid: "metrics-guid",
						Ports: []uint32{
							8080,
						},
						EgressRules: []*models.SecurityGroupRule{
							securityRule,
						},
					},

					Index: expectedIndexTwo,
				}
			})

			Context("when all LRP Auctions can be successfully translated to container specs", func() {
				BeforeEach(func() {
					lrpAuctionOne.DesiredLRP.RootFs = linuxRootFSURL
					lrpAuctionTwo.DesiredLRP.RootFs = "unsupported-arbitrary://still-goes-through"
				})

				It("makes the correct allocation requests for all LRP Auctions", func() {
					_, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					Expect(client.AllocateContainersArgsForCall(0)).To(ConsistOf(
						executor.Container{
							Guid: rep.LRPContainerGuid(lrpAuctionOne.DesiredLRP.ProcessGuid, expectedGuidOne),

							Tags: executor.Tags{
								rep.LifecycleTag:    rep.LRPLifecycle,
								rep.DomainTag:       lrpAuctionOne.DesiredLRP.Domain,
								rep.ProcessGuidTag:  lrpAuctionOne.DesiredLRP.ProcessGuid,
								rep.InstanceGuidTag: expectedGuidOne,
								rep.ProcessIndexTag: expectedIndexOneString,
							},

							MemoryMB:   int(lrpAuctionOne.DesiredLRP.MemoryMb),
							DiskMB:     int(lrpAuctionOne.DesiredLRP.DiskMb),
							CPUWeight:  uint(lrpAuctionOne.DesiredLRP.CpuWeight),
							RootFSPath: linuxPath,
							Privileged: lrpAuctionOne.DesiredLRP.Privileged,
							Ports:      []executor.PortMapping{{ContainerPort: 8080}},

							LogConfig: executor.LogConfig{
								Guid:       "log-guid",
								SourceName: "log-source",
								Index:      expectedIndexOne,
							},

							MetricsConfig: executor.MetricsConfig{
								Guid:  "metrics-guid",
								Index: expectedIndexOne,
							},

							Setup:   lrpAuctionOne.DesiredLRP.Setup,
							Action:  lrpAuctionOne.DesiredLRP.Action,
							Monitor: lrpAuctionOne.DesiredLRP.Monitor,

							Env: []executor.EnvironmentVariable{
								{Name: "INSTANCE_GUID", Value: expectedGuidOne},
								{Name: "INSTANCE_INDEX", Value: expectedIndexOneString},
								{Name: "CF_INSTANCE_GUID", Value: expectedGuidOne},
								{Name: "CF_INSTANCE_INDEX", Value: expectedIndexOneString},
								{Name: "var1", Value: "val1"},
								{Name: "var2", Value: "val2"},
							},
							EgressRules: []*models.SecurityGroupRule{
								securityRule,
							},
						},
						executor.Container{
							Guid: rep.LRPContainerGuid(lrpAuctionTwo.DesiredLRP.ProcessGuid, expectedGuidTwo),

							Tags: executor.Tags{
								rep.LifecycleTag:    rep.LRPLifecycle,
								rep.DomainTag:       lrpAuctionTwo.DesiredLRP.Domain,
								rep.ProcessGuidTag:  lrpAuctionTwo.DesiredLRP.ProcessGuid,
								rep.InstanceGuidTag: expectedGuidTwo,
								rep.ProcessIndexTag: expectedIndexTwoString,
							},

							MemoryMB:   int(lrpAuctionTwo.DesiredLRP.MemoryMb),
							DiskMB:     int(lrpAuctionTwo.DesiredLRP.DiskMb),
							CPUWeight:  uint(lrpAuctionTwo.DesiredLRP.CpuWeight),
							RootFSPath: "unsupported-arbitrary://still-goes-through",
							Privileged: lrpAuctionTwo.DesiredLRP.Privileged,
							Ports:      []executor.PortMapping{{ContainerPort: 8080}},

							LogConfig: executor.LogConfig{
								Guid:       "log-guid",
								SourceName: "log-source",
								Index:      expectedIndexTwo,
							},

							MetricsConfig: executor.MetricsConfig{
								Guid:  "metrics-guid",
								Index: expectedIndexTwo,
							},

							Setup:   lrpAuctionTwo.DesiredLRP.Setup,
							Action:  lrpAuctionTwo.DesiredLRP.Action,
							Monitor: lrpAuctionTwo.DesiredLRP.Monitor,

							Env: []executor.EnvironmentVariable{
								{Name: "INSTANCE_GUID", Value: expectedGuidTwo},
								{Name: "INSTANCE_INDEX", Value: expectedIndexTwoString},
								{Name: "CF_INSTANCE_GUID", Value: expectedGuidTwo},
								{Name: "CF_INSTANCE_INDEX", Value: expectedIndexTwoString},
								{Name: "var1", Value: "val1"},
								{Name: "var2", Value: "val2"},
							},
							EgressRules: []*models.SecurityGroupRule{
								securityRule,
							},
						},
					))

				})

				Context("when all containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{}, nil)
					})

					It("does not mark any LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork).To(BeZero())
					})
				})

				Context("when a container fails to be allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{
							rep.LRPContainerGuid(lrpAuctionOne.DesiredLRP.ProcessGuid, expectedGuidOne): commonErr.Error(),
						}, nil)
					})

					It("marks the corresponding LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.LRPs).To(ConsistOf(lrpAuctionOne))
					})
				})
			})

			Context("when an LRP Auction specifies a preloaded RootFSes for which it cannot determine a RootFS path", func() {
				BeforeEach(func() {
					lrpAuctionOne.DesiredLRP.RootFs = linuxRootFSURL
					lrpAuctionTwo.DesiredLRP.RootFs = "preloaded:not-on-cell"
				})

				It("only makes container allocation requests for the remaining LRP Auctions", func() {
					_, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					Expect(client.AllocateContainersArgsForCall(0)).To(ConsistOf(
						executor.Container{
							Guid: rep.LRPContainerGuid(lrpAuctionOne.DesiredLRP.ProcessGuid, expectedGuidOne),

							Tags: executor.Tags{
								rep.LifecycleTag:    rep.LRPLifecycle,
								rep.DomainTag:       lrpAuctionOne.DesiredLRP.Domain,
								rep.ProcessGuidTag:  lrpAuctionOne.DesiredLRP.ProcessGuid,
								rep.InstanceGuidTag: expectedGuidOne,
								rep.ProcessIndexTag: expectedIndexOneString,
							},

							MemoryMB:   int(lrpAuctionOne.DesiredLRP.MemoryMb),
							DiskMB:     int(lrpAuctionOne.DesiredLRP.DiskMb),
							CPUWeight:  uint(lrpAuctionOne.DesiredLRP.CpuWeight),
							RootFSPath: linuxPath,
							Privileged: lrpAuctionOne.DesiredLRP.Privileged,
							Ports:      []executor.PortMapping{{ContainerPort: 8080}},

							LogConfig: executor.LogConfig{
								Guid:       "log-guid",
								SourceName: "log-source",
								Index:      expectedIndexOne,
							},

							MetricsConfig: executor.MetricsConfig{
								Guid:  "metrics-guid",
								Index: expectedIndexOne,
							},

							Setup:   lrpAuctionOne.DesiredLRP.Setup,
							Action:  lrpAuctionOne.DesiredLRP.Action,
							Monitor: lrpAuctionOne.DesiredLRP.Monitor,

							Env: []executor.EnvironmentVariable{
								{Name: "INSTANCE_GUID", Value: expectedGuidOne},
								{Name: "INSTANCE_INDEX", Value: expectedIndexOneString},
								{Name: "CF_INSTANCE_GUID", Value: expectedGuidOne},
								{Name: "CF_INSTANCE_INDEX", Value: expectedIndexOneString},
								{Name: "var1", Value: "val1"},
								{Name: "var2", Value: "val2"},
							},
							EgressRules: []*models.SecurityGroupRule{
								securityRule,
							},
						},
					))

				})

				It("marks the LRP Auction as failed", func() {
					failedWork, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
					Expect(err).NotTo(HaveOccurred())
					Expect(failedWork.LRPs).To(ContainElement(lrpAuctionTwo))
				})

				Context("when all remaining containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{}, nil)
					})

					It("does not mark any additional LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.LRPs).To(ConsistOf(lrpAuctionTwo))
					})
				})

				Context("when a remaining container fails to be allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{
							rep.LRPContainerGuid(lrpAuctionOne.DesiredLRP.ProcessGuid, expectedGuidOne): commonErr.Error(),
						}, nil)
					})

					It("marks the corresponding LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.LRPs).To(ConsistOf(lrpAuctionOne, lrpAuctionTwo))
					})
				})
			})

			Context("when an LRP Auction specifies a blank RootFS URL", func() {
				BeforeEach(func() {
					lrpAuctionOne.DesiredLRP.RootFs = ""
				})

				It("makes the correct allocation request for it, passing along the blank path to the executor client", func() {
					_, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					Expect(client.AllocateContainersArgsForCall(0)).To(ConsistOf(
						executor.Container{
							Guid: rep.LRPContainerGuid(lrpAuctionOne.DesiredLRP.ProcessGuid, expectedGuidOne),

							Tags: executor.Tags{
								rep.LifecycleTag:    rep.LRPLifecycle,
								rep.DomainTag:       lrpAuctionOne.DesiredLRP.Domain,
								rep.ProcessGuidTag:  lrpAuctionOne.DesiredLRP.ProcessGuid,
								rep.InstanceGuidTag: expectedGuidOne,
								rep.ProcessIndexTag: expectedIndexOneString,
							},

							MemoryMB:   int(lrpAuctionOne.DesiredLRP.MemoryMb),
							DiskMB:     int(lrpAuctionOne.DesiredLRP.DiskMb),
							CPUWeight:  uint(lrpAuctionOne.DesiredLRP.CpuWeight),
							RootFSPath: "",
							Privileged: lrpAuctionOne.DesiredLRP.Privileged,
							Ports:      []executor.PortMapping{{ContainerPort: 8080}},

							LogConfig: executor.LogConfig{
								Guid:       "log-guid",
								SourceName: "log-source",
								Index:      expectedIndexOne,
							},

							MetricsConfig: executor.MetricsConfig{
								Guid:  "metrics-guid",
								Index: expectedIndexOne,
							},

							Setup:   lrpAuctionOne.DesiredLRP.Setup,
							Action:  lrpAuctionOne.DesiredLRP.Action,
							Monitor: lrpAuctionOne.DesiredLRP.Monitor,

							Env: []executor.EnvironmentVariable{
								{Name: "INSTANCE_GUID", Value: expectedGuidOne},
								{Name: "INSTANCE_INDEX", Value: expectedIndexOneString},
								{Name: "CF_INSTANCE_GUID", Value: expectedGuidOne},
								{Name: "CF_INSTANCE_INDEX", Value: expectedIndexOneString},
								{Name: "var1", Value: "val1"},
								{Name: "var2", Value: "val2"},
							},
							EgressRules: []*models.SecurityGroupRule{
								securityRule,
							},
						},
					))

				})
			})

			Context("when an LRP Auction specifies an invalid RootFS URL", func() {
				BeforeEach(func() {
					lrpAuctionOne.DesiredLRP.RootFs = linuxRootFSURL
					lrpAuctionTwo.DesiredLRP.RootFs = "%x"
				})

				It("only makes container allocation requests for the remaining LRP Auctions", func() {
					_, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					Expect(client.AllocateContainersArgsForCall(0)).To(ConsistOf(
						executor.Container{
							Guid: rep.LRPContainerGuid(lrpAuctionOne.DesiredLRP.ProcessGuid, expectedGuidOne),

							Tags: executor.Tags{
								rep.LifecycleTag:    rep.LRPLifecycle,
								rep.DomainTag:       lrpAuctionOne.DesiredLRP.Domain,
								rep.ProcessGuidTag:  lrpAuctionOne.DesiredLRP.ProcessGuid,
								rep.InstanceGuidTag: expectedGuidOne,
								rep.ProcessIndexTag: expectedIndexOneString,
							},

							MemoryMB:   int(lrpAuctionOne.DesiredLRP.MemoryMb),
							DiskMB:     int(lrpAuctionOne.DesiredLRP.DiskMb),
							CPUWeight:  uint(lrpAuctionOne.DesiredLRP.CpuWeight),
							RootFSPath: linuxPath,
							Privileged: lrpAuctionOne.DesiredLRP.Privileged,
							Ports:      []executor.PortMapping{{ContainerPort: 8080}},

							LogConfig: executor.LogConfig{
								Guid:       "log-guid",
								SourceName: "log-source",
								Index:      expectedIndexOne,
							},

							MetricsConfig: executor.MetricsConfig{
								Guid:  "metrics-guid",
								Index: expectedIndexOne,
							},

							Setup:   lrpAuctionOne.DesiredLRP.Setup,
							Action:  lrpAuctionOne.DesiredLRP.Action,
							Monitor: lrpAuctionOne.DesiredLRP.Monitor,

							Env: []executor.EnvironmentVariable{
								{Name: "INSTANCE_GUID", Value: expectedGuidOne},
								{Name: "INSTANCE_INDEX", Value: expectedIndexOneString},
								{Name: "CF_INSTANCE_GUID", Value: expectedGuidOne},
								{Name: "CF_INSTANCE_INDEX", Value: expectedIndexOneString},
								{Name: "var1", Value: "val1"},
								{Name: "var2", Value: "val2"},
							},
							EgressRules: []*models.SecurityGroupRule{
								securityRule,
							},
						},
					))

				})

				It("marks the LRP Auction as failed", func() {
					failedWork, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
					Expect(err).NotTo(HaveOccurred())
					Expect(failedWork.LRPs).To(ContainElement(lrpAuctionTwo))
				})

				Context("when all remaining containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{}, nil)
					})

					It("does not mark any additional LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.LRPs).To(ConsistOf(lrpAuctionTwo))
					})
				})

				Context("when a remaining container fails to be allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{
							rep.LRPContainerGuid(lrpAuctionOne.DesiredLRP.ProcessGuid, expectedGuidOne): commonErr.Error(),
						}, nil)
					})

					It("marks the corresponding LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.LRPs).To(ConsistOf(lrpAuctionOne, lrpAuctionTwo))
					})
				})
			})
		})

		Describe("starting tasks", func() {
			var task1, task2 *models.Task

			BeforeEach(func() {
				task1 = model_helpers.NewValidTask("the-task-guid-1")
				task2 = model_helpers.NewValidTask("the-task-guid-2")

				work = auctiontypes.Work{Tasks: []*models.Task{task}}
			})

			Context("when all Tasks can be successfully translated to container specs", func() {
				BeforeEach(func() {
					task1.RootFs = linuxRootFSURL
					task2.RootFs = "unsupported-arbitrary://still-goes-through"
				})

				It("makes the correct allocation requests for all Tasks", func() {
					_, err := cellRep.Perform(auctiontypes.Work{Tasks: []*models.Task{task1, task2}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					Expect(client.AllocateContainersArgsForCall(0)).To(ConsistOf(
						containerForTask(task1, linuxPath),
						containerForTask(task2, task2.RootFs),
					))

				})

				Context("when all containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{}, nil)
					})

					It("does not mark any Tasks as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{Tasks: []*models.Task{task1, task2}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork).To(BeZero())
					})
				})

				Context("when a container fails to be allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{
							"the-task-guid-1": commonErr.Error(),
						}, nil)
					})

					It("marks the corresponding Tasks as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{Tasks: []*models.Task{task1, task2}})
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
					_, err := cellRep.Perform(auctiontypes.Work{Tasks: []*models.Task{task1, task2}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					Expect(client.AllocateContainersArgsForCall(0)).To(ConsistOf(
						containerForTask(task1, linuxPath),
					))

				})

				It("marks the Task as failed", func() {
					failedWork, err := cellRep.Perform(auctiontypes.Work{Tasks: []*models.Task{task1, task2}})
					Expect(err).NotTo(HaveOccurred())
					Expect(failedWork.Tasks).To(ContainElement(task2))
				})

				Context("when all remaining containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{}, nil)
					})

					It("does not mark any additional Tasks as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{Tasks: []*models.Task{task1, task2}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.Tasks).To(ConsistOf(task2))
					})
				})

				Context("when a remaining container fails to be allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{
							"the-task-guid-1": commonErr.Error(),
						}, nil)
					})

					It("marks the corresponding Tasks as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{Tasks: []*models.Task{task1, task2}})
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
					_, err := cellRep.Perform(auctiontypes.Work{Tasks: []*models.Task{task1}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					Expect(client.AllocateContainersArgsForCall(0)).To(ConsistOf(
						containerForTask(task1, ""),
					))

				})
			})

			Context("when a Task specifies an invalid RootFS URL", func() {
				BeforeEach(func() {
					task1.RootFs = linuxRootFSURL
					task2.RootFs = "%x"
				})

				It("only makes container allocation requests for the remaining Tasks", func() {
					_, err := cellRep.Perform(auctiontypes.Work{Tasks: []*models.Task{task1, task2}})
					Expect(err).NotTo(HaveOccurred())

					Expect(client.AllocateContainersCallCount()).To(Equal(1))
					Expect(client.AllocateContainersArgsForCall(0)).To(ConsistOf(
						containerForTask(task1, linuxPath),
					))

				})

				It("marks the Task as failed", func() {
					failedWork, err := cellRep.Perform(auctiontypes.Work{Tasks: []*models.Task{task1, task2}})
					Expect(err).NotTo(HaveOccurred())
					Expect(failedWork.Tasks).To(ContainElement(task2))
				})

				Context("when all remaining containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{}, nil)
					})

					It("does not mark any additional LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{Tasks: []*models.Task{task1, task2}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.Tasks).To(ConsistOf(task2))
					})
				})

				Context("when a remaining container fails to be allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{
							"the-task-guid-1": commonErr.Error(),
						}, nil)
					})

					It("marks the corresponding Tasks as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{Tasks: []*models.Task{task1, task2}})
						Expect(err).NotTo(HaveOccurred())
						Expect(failedWork.Tasks).To(ConsistOf(task1, task2))
					})
				})
			})
		})
	})
})

func containerForTask(task *models.Task, rootFSPath string) executor.Container {
	return executor.Container{
		Guid: task.TaskGuid,

		Tags: executor.Tags{
			rep.LifecycleTag:  rep.TaskLifecycle,
			rep.DomainTag:     task.Domain,
			rep.ResultFileTag: task.ResultFile,
		},

		Action: task.Action,
		Env: []executor.EnvironmentVariable{
			{Name: "FOO", Value: "BAR"},
		},

		LogConfig: executor.LogConfig{
			Guid:       task.LogGuid,
			SourceName: task.LogSource,
		},

		MetricsConfig: executor.MetricsConfig{
			Guid: task.MetricsGuid,
		},

		MemoryMB:    int(task.MemoryMb),
		DiskMB:      int(task.DiskMb),
		CPUWeight:   uint(task.CpuWeight),
		RootFSPath:  rootFSPath,
		Privileged:  task.Privileged,
		EgressRules: task.EgressRules,
	}
}
