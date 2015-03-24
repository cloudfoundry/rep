package auction_cell_rep_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	executor "github.com/cloudfoundry-incubator/executor"
	fake_client "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	. "github.com/cloudfoundry-incubator/rep/auction_cell_rep"
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

		commonErr = errors.New("Failed to fetch")
	})

	JustBeforeEach(func() {
		cellRep = New(expectedCellID, "lucid64", "the-zone", fakeGenerateContainerGuid, bbs, client, evacuationReporter, logger)
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
						RootFS:      "some-root-fs",
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
					RootFS:     "some-root-fs",
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
						RootFS:      "some-root-fs",
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
						RootFS:      "some-root-fs",
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

				work = auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuctionOne, lrpAuctionTwo}}
			})

			It("should allocate the batch of containers", func() {
				_, err := cellRep.Perform(work)
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
						RootFSPath: "some-root-fs",
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
						RootFSPath: "some-root-fs",
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

			Context("when allocation succeeds", func() {
				BeforeEach(func() {
					client.AllocateContainersReturns(map[string]string{}, nil)
				})

				It("responds successfully", func() {
					failedWork, err := cellRep.Perform(work)
					Ω(err).ShouldNot(HaveOccurred())
					Ω(failedWork).Should(BeZero())
				})
			})

			Context("when allocation fails", func() {
				BeforeEach(func() {
					client.AllocateContainersReturns(map[string]string{
						rep.LRPContainerGuid(lrpAuctionOne.DesiredLRP.ProcessGuid, expectedGuidOne): commonErr.Error(),
						rep.LRPContainerGuid(lrpAuctionTwo.DesiredLRP.ProcessGuid, expectedGuidTwo): commonErr.Error(),
					}, nil)
				})

				It("adds to the failed work", func() {
					failedWork, err := cellRep.Perform(work)
					Ω(err).ShouldNot(HaveOccurred())
					Ω(failedWork.LRPs).Should(ConsistOf(lrpAuctionOne, lrpAuctionTwo))
				})
			})
		})

		Describe("starting tasks", func() {
			var task models.Task
			var securityRule models.SecurityGroupRule

			BeforeEach(func() {
				securityRule = models.SecurityGroupRule{
					Protocol:     "tcp",
					Destinations: []string{"0.0.0.0/0"},
					Ports:        []uint16{80},
				}
				task = models.Task{
					Domain:      "tests",
					TaskGuid:    "the-task-guid",
					DiskMB:      1024,
					MemoryMB:    2048,
					RootFS:      "the-root-fs",
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

			It("should allocate a container", func() {
				_, err := cellRep.Perform(work)
				Ω(err).ShouldNot(HaveOccurred())

				Ω(client.AllocateContainersCallCount()).Should(Equal(1))
				Ω(client.AllocateContainersArgsForCall(0)).Should(Equal([]executor.Container{
					{
						Guid: task.TaskGuid,

						Tags: executor.Tags{
							rep.LifecycleTag:  rep.TaskLifecycle,
							rep.DomainTag:     task.Domain,
							rep.ResultFileTag: task.ResultFile,
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

						MemoryMB:   task.MemoryMB,
						DiskMB:     task.DiskMB,
						CPUWeight:  task.CPUWeight,
						RootFSPath: task.RootFS,
						Privileged: task.Privileged,
						EgressRules: []models.SecurityGroupRule{
							securityRule,
						},
					},
				}))
			})

			Context("when allocation succeeds", func() {
				BeforeEach(func() {
					client.AllocateContainersReturns(map[string]string{}, nil)
				})

				It("responds successfully before starting the task in the BBS", func() {
					failedWork, err := cellRep.Perform(work)
					Ω(err).ShouldNot(HaveOccurred())
					Ω(failedWork).Should(BeZero())
				})
			})

			Context("when the container fails to allocate", func() {
				BeforeEach(func() {
					client.AllocateContainersReturns(map[string]string{
						task.TaskGuid: commonErr.Error(),
					}, nil)
				})

				It("adds to the failed work", func() {
					failedWork, err := cellRep.Perform(work)
					Ω(err).ShouldNot(HaveOccurred(), "note: we don't error")
					Ω(failedWork.Tasks).Should(ConsistOf(task))
				})

				It("doesn't start the task in the BBS", func() {
					cellRep.Perform(work)
					Consistently(bbs.StartTaskCallCount).Should(Equal(0))
				})

				It("doesn't run the container", func() {
					cellRep.Perform(work)
					Consistently(client.RunContainerCallCount).Should(Equal(0))
				})
			})
		})
	})
})
