package auction_cell_rep_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	executor "github.com/cloudfoundry-incubator/executor"
	fake_client "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/auction_cell_rep"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context/fake_evacuation_context"
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
	var logger *lagertest.TestLogger
	var evacuationReporter *fake_evacuation_context.FakeEvacuationReporter

	const expectedCellID = "some-cell-id"
	var expectedGuid string
	var expectedGuidError error
	var fakeGenerateContainerGuid func() (string, error)

	const lucidStack = "lucid64"
	const lucidPath = "/data/rootfs/lucid64"
	var lucidRootFSURL string

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
		lucidRootFSURL = models.PreloadedRootFS(lucidStack)

		commonErr = errors.New("Failed to fetch")
	})

	JustBeforeEach(func() {
		cellRep = auction_cell_rep.New(expectedCellID, rep.StackPathMap{lucidStack: lucidPath}, []string{"docker"}, "the-zone", fakeGenerateContainerGuid, bbs, client, evacuationReporter, logger)
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
			Ω(err).ShouldNot(HaveOccurred())

			Ω(client.ListContainersArgsForCall(0)).Should(Equal(executor.Tags{
				rep.LifecycleTag: rep.LRPLifecycle,
			}))

			Ω(state.Evacuating).Should(BeTrue())
			Ω(state.RootFSProviders).Should(Equal(auctiontypes.RootFSProviders{
				models.PreloadedRootFSScheme: auctiontypes.NewFixedSetRootFSProvider("lucid64"),
				"docker":                     auctiontypes.ArbitraryRootFSProvider{},
			}))
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

	Describe("Perform", func() {
		var (
			work       auctiontypes.Work
			lrpAuction auctiontypes.LRPAuction
			task       models.Task

			expectedIndex = 1
		)

		Context("when evacuating", func() {
			BeforeEach(func() {
				evacuationReporter.EvacuatingReturns(true)

				lrpAuction = auctiontypes.LRPAuction{
					DesiredLRP: models.DesiredLRP{
						Domain:      "tests",
						RootFS:      lucidRootFSURL,
						ProcessGuid: "process-guid",
						DiskMB:      1024,
						MemoryMB:    2048,
						CPUWeight:   42,
						Privileged:  true,
						LogGuid:     "log-guid",
					},

					Index: expectedIndex,
				}

				task = models.Task{
					Domain:     "tests",
					TaskGuid:   "the-task-guid",
					RootFS:     lucidRootFSURL,
					DiskMB:     1024,
					MemoryMB:   2048,
					Privileged: true,
					CPUWeight:  10,
				}

				work = auctiontypes.Work{
					LRPs:  []auctiontypes.LRPAuction{lrpAuction},
					Tasks: []models.Task{task},
				}
			})

			It("returns all work it was given", func() {
				Ω(cellRep.Perform(work)).Should(Equal(work))
			})
		})

		Describe("performing starts", func() {
			var lrpAuctionOne auctiontypes.LRPAuction
			var lrpAuctionTwo auctiontypes.LRPAuction
			var securityRule models.SecurityGroupRule
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

				securityRule = models.SecurityGroupRule{
					Protocol:     "tcp",
					Destinations: []string{"0.0.0.0/0"},
					PortRange: &models.PortRange{
						Start: 1,
						End:   1024,
					},
				}
				lrpAuctionOne = auctiontypes.LRPAuction{
					DesiredLRP: models.DesiredLRP{
						Domain:      "tests",
						ProcessGuid: "process-guid",
						DiskMB:      1024,
						MemoryMB:    2048,
						CPUWeight:   42,
						Privileged:  true,
						EnvironmentVariables: []models.EnvironmentVariable{
							{Name: "var1", Value: "val1"},
							{Name: "var2", Value: "val2"},
						},
						Action: &models.DownloadAction{
							From: "http://example.com/something",
							To:   "/something",
						},
						LogGuid:     "log-guid",
						LogSource:   "log-source",
						MetricsGuid: "metrics-guid",
						Ports: []uint16{
							8080,
						},
						EgressRules: []models.SecurityGroupRule{
							securityRule,
						},
					},

					Index: expectedIndexOne,
				}

				lrpAuctionTwo = auctiontypes.LRPAuction{
					DesiredLRP: models.DesiredLRP{
						Domain:      "tests",
						ProcessGuid: "process-guid",
						DiskMB:      1024,
						MemoryMB:    2048,
						CPUWeight:   42,
						Privileged:  true,
						EnvironmentVariables: []models.EnvironmentVariable{
							{Name: "var1", Value: "val1"},
							{Name: "var2", Value: "val2"},
						},
						Action: &models.DownloadAction{
							From: "http://example.com/something",
							To:   "/something",
						},
						LogGuid:     "log-guid",
						LogSource:   "log-source",
						MetricsGuid: "metrics-guid",
						Ports: []uint16{
							8080,
						},
						EgressRules: []models.SecurityGroupRule{
							securityRule,
						},
					},

					Index: expectedIndexTwo,
				}
			})

			Context("when all LRP Auctions can be successfully translated to container specs", func() {
				BeforeEach(func() {
					lrpAuctionOne.DesiredLRP.RootFS = lucidRootFSURL
					lrpAuctionTwo.DesiredLRP.RootFS = "unsupported-arbitrary://still-goes-through"
				})

				It("makes the correct allocation requests for all LRP Auctions", func() {
					_, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
					Ω(err).ShouldNot(HaveOccurred())

					Ω(client.AllocateContainersCallCount()).Should(Equal(1))
					Ω(client.AllocateContainersArgsForCall(0)).Should(ConsistOf(
						executor.Container{
							Guid: rep.LRPContainerGuid(lrpAuctionOne.DesiredLRP.ProcessGuid, expectedGuidOne),

							Tags: executor.Tags{
								rep.LifecycleTag:    rep.LRPLifecycle,
								rep.DomainTag:       lrpAuctionOne.DesiredLRP.Domain,
								rep.ProcessGuidTag:  lrpAuctionOne.DesiredLRP.ProcessGuid,
								rep.InstanceGuidTag: expectedGuidOne,
								rep.ProcessIndexTag: expectedIndexOneString,
							},

							MemoryMB:   lrpAuctionOne.DesiredLRP.MemoryMB,
							DiskMB:     lrpAuctionOne.DesiredLRP.DiskMB,
							CPUWeight:  lrpAuctionOne.DesiredLRP.CPUWeight,
							RootFSPath: lucidPath,
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
								{Name: "var1", Value: "val1"},
								{Name: "var2", Value: "val2"},
							},
							EgressRules: []models.SecurityGroupRule{
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

							MemoryMB:   lrpAuctionTwo.DesiredLRP.MemoryMB,
							DiskMB:     lrpAuctionTwo.DesiredLRP.DiskMB,
							CPUWeight:  lrpAuctionTwo.DesiredLRP.CPUWeight,
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
								{Name: "var1", Value: "val1"},
								{Name: "var2", Value: "val2"},
							},
							EgressRules: []models.SecurityGroupRule{
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
						Ω(err).ShouldNot(HaveOccurred())
						Ω(failedWork).Should(BeZero())
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
						Ω(err).ShouldNot(HaveOccurred())
						Ω(failedWork.LRPs).Should(ConsistOf(lrpAuctionOne))
					})
				})
			})

			Context("when an LRP Auction specifies a preloaded RootFSes for which it cannot determine a RootFS path", func() {
				BeforeEach(func() {
					lrpAuctionOne.DesiredLRP.RootFS = lucidRootFSURL
					lrpAuctionTwo.DesiredLRP.RootFS = "preloaded:not-on-cell"
				})

				It("only makes container allocation requests for the remaining LRP Auctions", func() {
					_, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
					Ω(err).ShouldNot(HaveOccurred())

					Ω(client.AllocateContainersCallCount()).Should(Equal(1))
					Ω(client.AllocateContainersArgsForCall(0)).Should(ConsistOf(
						executor.Container{
							Guid: rep.LRPContainerGuid(lrpAuctionOne.DesiredLRP.ProcessGuid, expectedGuidOne),

							Tags: executor.Tags{
								rep.LifecycleTag:    rep.LRPLifecycle,
								rep.DomainTag:       lrpAuctionOne.DesiredLRP.Domain,
								rep.ProcessGuidTag:  lrpAuctionOne.DesiredLRP.ProcessGuid,
								rep.InstanceGuidTag: expectedGuidOne,
								rep.ProcessIndexTag: expectedIndexOneString,
							},

							MemoryMB:   lrpAuctionOne.DesiredLRP.MemoryMB,
							DiskMB:     lrpAuctionOne.DesiredLRP.DiskMB,
							CPUWeight:  lrpAuctionOne.DesiredLRP.CPUWeight,
							RootFSPath: lucidPath,
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
								{Name: "var1", Value: "val1"},
								{Name: "var2", Value: "val2"},
							},
							EgressRules: []models.SecurityGroupRule{
								securityRule,
							},
						},
					))
				})

				It("marks the LRP Auction as failed", func() {
					failedWork, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
					Ω(err).ShouldNot(HaveOccurred())
					Ω(failedWork.LRPs).Should(ContainElement(lrpAuctionTwo))
				})

				Context("when all remaining containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{}, nil)
					})

					It("does not mark any additional LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
						Ω(err).ShouldNot(HaveOccurred())
						Ω(failedWork.LRPs).Should(ConsistOf(lrpAuctionTwo))
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
						Ω(err).ShouldNot(HaveOccurred())
						Ω(failedWork.LRPs).Should(ConsistOf(lrpAuctionOne, lrpAuctionTwo))
					})
				})
			})

			Context("when an LRP Auction specifies a blank RootFS URL", func() {
				BeforeEach(func() {
					lrpAuctionOne.DesiredLRP.RootFS = ""
				})

				It("makes the correct allocation request for it, passing along the blank path to the executor client", func() {
					_, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne}})
					Ω(err).ShouldNot(HaveOccurred())

					Ω(client.AllocateContainersCallCount()).Should(Equal(1))
					Ω(client.AllocateContainersArgsForCall(0)).Should(ConsistOf(
						executor.Container{
							Guid: rep.LRPContainerGuid(lrpAuctionOne.DesiredLRP.ProcessGuid, expectedGuidOne),

							Tags: executor.Tags{
								rep.LifecycleTag:    rep.LRPLifecycle,
								rep.DomainTag:       lrpAuctionOne.DesiredLRP.Domain,
								rep.ProcessGuidTag:  lrpAuctionOne.DesiredLRP.ProcessGuid,
								rep.InstanceGuidTag: expectedGuidOne,
								rep.ProcessIndexTag: expectedIndexOneString,
							},

							MemoryMB:   lrpAuctionOne.DesiredLRP.MemoryMB,
							DiskMB:     lrpAuctionOne.DesiredLRP.DiskMB,
							CPUWeight:  lrpAuctionOne.DesiredLRP.CPUWeight,
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
								{Name: "var1", Value: "val1"},
								{Name: "var2", Value: "val2"},
							},
							EgressRules: []models.SecurityGroupRule{
								securityRule,
							},
						},
					))
				})
			})

			Context("when an LRP Auction specifies an invalid RootFS URL", func() {
				BeforeEach(func() {
					lrpAuctionOne.DesiredLRP.RootFS = lucidRootFSURL
					lrpAuctionTwo.DesiredLRP.RootFS = "%x"
				})

				It("only makes container allocation requests for the remaining LRP Auctions", func() {
					_, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
					Ω(err).ShouldNot(HaveOccurred())

					Ω(client.AllocateContainersCallCount()).Should(Equal(1))
					Ω(client.AllocateContainersArgsForCall(0)).Should(ConsistOf(
						executor.Container{
							Guid: rep.LRPContainerGuid(lrpAuctionOne.DesiredLRP.ProcessGuid, expectedGuidOne),

							Tags: executor.Tags{
								rep.LifecycleTag:    rep.LRPLifecycle,
								rep.DomainTag:       lrpAuctionOne.DesiredLRP.Domain,
								rep.ProcessGuidTag:  lrpAuctionOne.DesiredLRP.ProcessGuid,
								rep.InstanceGuidTag: expectedGuidOne,
								rep.ProcessIndexTag: expectedIndexOneString,
							},

							MemoryMB:   lrpAuctionOne.DesiredLRP.MemoryMB,
							DiskMB:     lrpAuctionOne.DesiredLRP.DiskMB,
							CPUWeight:  lrpAuctionOne.DesiredLRP.CPUWeight,
							RootFSPath: lucidPath,
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
								{Name: "var1", Value: "val1"},
								{Name: "var2", Value: "val2"},
							},
							EgressRules: []models.SecurityGroupRule{
								securityRule,
							},
						},
					))
				})

				It("marks the LRP Auction as failed", func() {
					failedWork, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
					Ω(err).ShouldNot(HaveOccurred())
					Ω(failedWork.LRPs).Should(ContainElement(lrpAuctionTwo))
				})

				Context("when all remaining containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{}, nil)
					})

					It("does not mark any additional LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}})
						Ω(err).ShouldNot(HaveOccurred())
						Ω(failedWork.LRPs).Should(ConsistOf(lrpAuctionTwo))
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
						Ω(err).ShouldNot(HaveOccurred())
						Ω(failedWork.LRPs).Should(ConsistOf(lrpAuctionOne, lrpAuctionTwo))
					})
				})
			})
		})

		Describe("starting tasks", func() {
			var task1, task2 models.Task
			var securityRule models.SecurityGroupRule

			BeforeEach(func() {
				securityRule = models.SecurityGroupRule{
					Protocol:     "tcp",
					Destinations: []string{"0.0.0.0/0"},
					Ports:        []uint16{80},
				}
				task1 = models.Task{
					Domain:      "tests",
					TaskGuid:    "the-task-guid-1",
					DiskMB:      1024,
					MemoryMB:    2048,
					Privileged:  true,
					CPUWeight:   10,
					LogGuid:     "log-guid",
					LogSource:   "log-source",
					MetricsGuid: "metrics-guid",
					Action: &models.RunAction{
						Path: "date",
					},
					EnvironmentVariables: []models.EnvironmentVariable{
						{Name: "FOO", Value: "BAR"},
					},
					EgressRules: []models.SecurityGroupRule{
						securityRule,
					},
				}
				task2 = models.Task{
					Domain:      "tests",
					TaskGuid:    "the-task-guid-2",
					DiskMB:      1024,
					MemoryMB:    2048,
					Privileged:  true,
					CPUWeight:   10,
					LogGuid:     "log-guid",
					LogSource:   "log-source",
					MetricsGuid: "metrics-guid",
					Action: &models.RunAction{
						Path: "date",
					},
					EnvironmentVariables: []models.EnvironmentVariable{
						{Name: "FOO", Value: "BAR"},
					},
					EgressRules: []models.SecurityGroupRule{
						securityRule,
					},
				}
				work = auctiontypes.Work{Tasks: []models.Task{task}}
			})

			Context("when all Tasks can be successfully translated to container specs", func() {
				BeforeEach(func() {
					task1.RootFS = lucidRootFSURL
					task2.RootFS = "unsupported-arbitrary://still-goes-through"
				})

				It("makes the correct allocation requests for all Tasks", func() {
					_, err := cellRep.Perform(auctiontypes.Work{Tasks: []models.Task{task1, task2}})
					Ω(err).ShouldNot(HaveOccurred())

					Ω(client.AllocateContainersCallCount()).Should(Equal(1))
					Ω(client.AllocateContainersArgsForCall(0)).Should(ConsistOf(
						executor.Container{
							Guid: "the-task-guid-1",

							Tags: executor.Tags{
								rep.LifecycleTag:  rep.TaskLifecycle,
								rep.DomainTag:     task1.Domain,
								rep.ResultFileTag: task1.ResultFile,
							},

							Action: &models.RunAction{
								Path: "date",
							},
							Env: []executor.EnvironmentVariable{
								{Name: "FOO", Value: "BAR"},
							},

							LogConfig: executor.LogConfig{
								Guid:       "log-guid",
								SourceName: "log-source",
							},

							MetricsConfig: executor.MetricsConfig{
								Guid: "metrics-guid",
							},

							MemoryMB:   task1.MemoryMB,
							DiskMB:     task1.DiskMB,
							CPUWeight:  task1.CPUWeight,
							RootFSPath: lucidPath,
							Privileged: task1.Privileged,
							EgressRules: []models.SecurityGroupRule{
								securityRule,
							},
						},
						executor.Container{
							Guid: "the-task-guid-2",

							Tags: executor.Tags{
								rep.LifecycleTag:  rep.TaskLifecycle,
								rep.DomainTag:     task2.Domain,
								rep.ResultFileTag: task2.ResultFile,
							},

							Action: &models.RunAction{
								Path: "date",
							},
							Env: []executor.EnvironmentVariable{
								{Name: "FOO", Value: "BAR"},
							},

							LogConfig: executor.LogConfig{
								Guid:       "log-guid",
								SourceName: "log-source",
							},

							MetricsConfig: executor.MetricsConfig{
								Guid: "metrics-guid",
							},

							MemoryMB:   task2.MemoryMB,
							DiskMB:     task2.DiskMB,
							CPUWeight:  task2.CPUWeight,
							RootFSPath: "unsupported-arbitrary://still-goes-through",
							Privileged: task2.Privileged,
							EgressRules: []models.SecurityGroupRule{
								securityRule,
							},
						},
					))
				})

				Context("when all containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{}, nil)
					})

					It("does not mark any Tasks as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{Tasks: []models.Task{task1, task2}})
						Ω(err).ShouldNot(HaveOccurred())
						Ω(failedWork).Should(BeZero())
					})
				})

				Context("when a container fails to be allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{
							"the-task-guid-1": commonErr.Error(),
						}, nil)
					})

					It("marks the corresponding Tasks as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{Tasks: []models.Task{task1, task2}})
						Ω(err).ShouldNot(HaveOccurred())
						Ω(failedWork.Tasks).Should(ConsistOf(task1))
					})
				})
			})

			Context("when a Task specifies a preloaded RootFSes for which it cannot determine a RootFS path", func() {
				BeforeEach(func() {
					task1.RootFS = lucidRootFSURL
					task2.RootFS = "preloaded:not-on-cell"
				})

				It("only makes container allocation requests for the remaining Tasks", func() {
					_, err := cellRep.Perform(auctiontypes.Work{Tasks: []models.Task{task1, task2}})
					Ω(err).ShouldNot(HaveOccurred())

					Ω(client.AllocateContainersCallCount()).Should(Equal(1))
					Ω(client.AllocateContainersArgsForCall(0)).Should(ConsistOf(
						executor.Container{
							Guid: "the-task-guid-1",

							Tags: executor.Tags{
								rep.LifecycleTag:  rep.TaskLifecycle,
								rep.DomainTag:     task1.Domain,
								rep.ResultFileTag: task1.ResultFile,
							},

							Action: &models.RunAction{
								Path: "date",
							},
							Env: []executor.EnvironmentVariable{
								{Name: "FOO", Value: "BAR"},
							},

							LogConfig: executor.LogConfig{
								Guid:       "log-guid",
								SourceName: "log-source",
							},

							MetricsConfig: executor.MetricsConfig{
								Guid: "metrics-guid",
							},

							MemoryMB:   task1.MemoryMB,
							DiskMB:     task1.DiskMB,
							CPUWeight:  task1.CPUWeight,
							RootFSPath: lucidPath,
							Privileged: task1.Privileged,
							EgressRules: []models.SecurityGroupRule{
								securityRule,
							},
						},
					))
				})

				It("marks the Task as failed", func() {
					failedWork, err := cellRep.Perform(auctiontypes.Work{Tasks: []models.Task{task1, task2}})
					Ω(err).ShouldNot(HaveOccurred())
					Ω(failedWork.Tasks).Should(ContainElement(task2))
				})

				Context("when all remaining containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{}, nil)
					})

					It("does not mark any additional Tasks as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{Tasks: []models.Task{task1, task2}})
						Ω(err).ShouldNot(HaveOccurred())
						Ω(failedWork.Tasks).Should(ConsistOf(task2))
					})
				})

				Context("when a remaining container fails to be allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{
							"the-task-guid-1": commonErr.Error(),
						}, nil)
					})

					It("marks the corresponding Tasks as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{Tasks: []models.Task{task1, task2}})
						Ω(err).ShouldNot(HaveOccurred())
						Ω(failedWork.Tasks).Should(ConsistOf(task1, task2))
					})
				})
			})

			Context("when a Task specifies a blank RootFS URL", func() {
				BeforeEach(func() {
					task1.RootFS = ""
				})

				It("makes the correct allocation request for it, passing along the blank path to the executor client", func() {
					_, err := cellRep.Perform(auctiontypes.Work{Tasks: []models.Task{task1}})
					Ω(err).ShouldNot(HaveOccurred())

					Ω(client.AllocateContainersCallCount()).Should(Equal(1))
					Ω(client.AllocateContainersArgsForCall(0)).Should(ConsistOf(
						executor.Container{
							Guid: "the-task-guid-1",

							Tags: executor.Tags{
								rep.LifecycleTag:  rep.TaskLifecycle,
								rep.DomainTag:     task1.Domain,
								rep.ResultFileTag: task1.ResultFile,
							},

							Action: &models.RunAction{
								Path: "date",
							},
							Env: []executor.EnvironmentVariable{
								{Name: "FOO", Value: "BAR"},
							},

							LogConfig: executor.LogConfig{
								Guid:       "log-guid",
								SourceName: "log-source",
							},

							MetricsConfig: executor.MetricsConfig{
								Guid: "metrics-guid",
							},

							MemoryMB:   task1.MemoryMB,
							DiskMB:     task1.DiskMB,
							CPUWeight:  task1.CPUWeight,
							RootFSPath: "",
							Privileged: task1.Privileged,
							EgressRules: []models.SecurityGroupRule{
								securityRule,
							},
						},
					))
				})
			})

			Context("when a Task specifies an invalid RootFS URL", func() {
				BeforeEach(func() {
					task1.RootFS = lucidRootFSURL
					task2.RootFS = "%x"
				})

				It("only makes container allocation requests for the remaining Tasks", func() {
					_, err := cellRep.Perform(auctiontypes.Work{Tasks: []models.Task{task1, task2}})
					Ω(err).ShouldNot(HaveOccurred())

					Ω(client.AllocateContainersCallCount()).Should(Equal(1))
					Ω(client.AllocateContainersArgsForCall(0)).Should(ConsistOf(
						executor.Container{
							Guid: "the-task-guid-1",

							Tags: executor.Tags{
								rep.LifecycleTag:  rep.TaskLifecycle,
								rep.DomainTag:     task1.Domain,
								rep.ResultFileTag: task1.ResultFile,
							},

							Action: &models.RunAction{
								Path: "date",
							},
							Env: []executor.EnvironmentVariable{
								{Name: "FOO", Value: "BAR"},
							},

							LogConfig: executor.LogConfig{
								Guid:       "log-guid",
								SourceName: "log-source",
							},

							MetricsConfig: executor.MetricsConfig{
								Guid: "metrics-guid",
							},

							MemoryMB:   task1.MemoryMB,
							DiskMB:     task1.DiskMB,
							CPUWeight:  task1.CPUWeight,
							RootFSPath: lucidPath,
							Privileged: task1.Privileged,
							EgressRules: []models.SecurityGroupRule{
								securityRule,
							},
						},
					))
				})

				It("marks the Task as failed", func() {
					failedWork, err := cellRep.Perform(auctiontypes.Work{Tasks: []models.Task{task1, task2}})
					Ω(err).ShouldNot(HaveOccurred())
					Ω(failedWork.Tasks).Should(ContainElement(task2))
				})

				Context("when all remaining containers can be successfully allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{}, nil)
					})

					It("does not mark any additional LRP Auctions as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{Tasks: []models.Task{task1, task2}})
						Ω(err).ShouldNot(HaveOccurred())
						Ω(failedWork.Tasks).Should(ConsistOf(task2))
					})
				})

				Context("when a remaining container fails to be allocated", func() {
					BeforeEach(func() {
						client.AllocateContainersReturns(map[string]string{
							"the-task-guid-1": commonErr.Error(),
						}, nil)
					})

					It("marks the corresponding Tasks as failed", func() {
						failedWork, err := cellRep.Perform(auctiontypes.Work{Tasks: []models.Task{task1, task2}})
						Ω(err).ShouldNot(HaveOccurred())
						Ω(failedWork.Tasks).Should(ConsistOf(task1, task2))
					})
				})
			})
		})
	})
})
