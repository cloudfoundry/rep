package auction_cell_rep_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	executor "github.com/cloudfoundry-incubator/executor"
	fake_client "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	. "github.com/cloudfoundry-incubator/rep/auction_cell_rep"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
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

	const expectedCellID = "some-cell-id"
	var expectedGuid string
	var expectedGuidError error
	var fakeGenerateContainerGuid = func() (string, error) {
		return expectedGuid, expectedGuidError
	}

	BeforeEach(func() {
		expectedGuid = "container-guid"
		expectedGuidError = nil
		client = new(fake_client.FakeClient)
		bbs = &fake_bbs.FakeRepBBS{}
		logger = lagertest.NewTestLogger("test")
		cellRep = New(expectedCellID, "lucid64", "the-zone", fakeGenerateContainerGuid, bbs, client, logger)
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

	Describe("performing work", func() {
		var work auctiontypes.Work
		Describe("performing starts", func() {
			var lrpAuction auctiontypes.LRPAuction
			var expectedLRPKey models.ActualLRPKey
			var expectedLRPContainerKey models.ActualLRPContainerKey
			var expectedIndex = 2
			const expectedIndexString = "2"

			BeforeEach(func() {
				lrpAuction = auctiontypes.LRPAuction{
					DesiredLRP: models.DesiredLRP{
						Domain:      "tests",
						RootFSPath:  "some-root-fs",
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
						LogGuid: "log-guid",
						Ports: []uint32{
							8080,
						},
					},

					Index: expectedIndex,
				}

				expectedLRPKey = models.NewActualLRPKey(lrpAuction.DesiredLRP.ProcessGuid, lrpAuction.Index, lrpAuction.DesiredLRP.Domain)
				expectedLRPContainerKey = models.NewActualLRPContainerKey(expectedGuid, expectedCellID)
				work = auctiontypes.Work{LRPs: []auctiontypes.LRPAuction{lrpAuction}}
			})

			It("should allocate a container", func() {
				_, err := cellRep.Perform(work)
				Ω(err).ShouldNot(HaveOccurred())

				Ω(client.AllocateContainerCallCount()).Should(Equal(1))

				Ω(client.AllocateContainerArgsForCall(0)).Should(Equal(executor.Container{
					Guid: expectedGuid,

					Tags: executor.Tags{
						rep.LifecycleTag:    rep.LRPLifecycle,
						rep.DomainTag:       lrpAuction.DesiredLRP.Domain,
						rep.ProcessGuidTag:  lrpAuction.DesiredLRP.ProcessGuid,
						rep.ProcessIndexTag: expectedIndexString,
					},

					MemoryMB:   lrpAuction.DesiredLRP.MemoryMB,
					DiskMB:     lrpAuction.DesiredLRP.DiskMB,
					CPUWeight:  lrpAuction.DesiredLRP.CPUWeight,
					RootFSPath: "some-root-fs",
					Privileged: lrpAuction.DesiredLRP.Privileged,
					Ports:      []executor.PortMapping{{ContainerPort: 8080}},
					Log:        executor.LogConfig{Guid: "log-guid", Index: &expectedIndex},

					Setup:   lrpAuction.DesiredLRP.Setup,
					Action:  lrpAuction.DesiredLRP.Action,
					Monitor: lrpAuction.DesiredLRP.Monitor,

					Env: []executor.EnvironmentVariable{
						{Name: "INSTANCE_GUID", Value: expectedGuid},
						{Name: "INSTANCE_INDEX", Value: expectedIndexString},
						{Name: "var1", Value: "val1"},
						{Name: "var2", Value: "val2"},
					},
				}))
			})

			Context("when allocation succeeds", func() {
				BeforeEach(func() {
					client.AllocateContainerReturns(executor.Container{}, nil)
				})

				It("responds successfully", func() {
					failedWork, err := cellRep.Perform(work)
					Ω(err).ShouldNot(HaveOccurred())
					Ω(failedWork).Should(BeZero())
				})
			})

			Context("when allocation fails", func() {
				BeforeEach(func() {
					client.AllocateContainerReturns(executor.Container{}, commonErr)
				})

				It("adds to the failed work", func() {
					failedWork, err := cellRep.Perform(work)
					Ω(err).ShouldNot(HaveOccurred())
					Ω(failedWork.LRPs).Should(ConsistOf(lrpAuction))
				})
			})
		})

		Describe("starting tasks", func() {
			var task models.Task

			BeforeEach(func() {
				task = models.Task{
					Domain:     "tests",
					TaskGuid:   "the-task-guid",
					Stack:      "lucid64",
					DiskMB:     1024,
					MemoryMB:   2048,
					RootFSPath: "the-root-fs-path",
					Privileged: true,
					CPUWeight:  10,
					Action: &models.RunAction{
						Path: "date",
					},
					EnvironmentVariables: []models.EnvironmentVariable{
						{Name: "FOO", Value: "BAR"},
					},
				}

				work = auctiontypes.Work{Tasks: []models.Task{task}}
			})

			It("should allocate a container", func() {
				_, err := cellRep.Perform(work)
				Ω(err).ShouldNot(HaveOccurred())

				Ω(client.AllocateContainerCallCount()).Should(Equal(1))

				Ω(client.AllocateContainerArgsForCall(0)).Should(Equal(executor.Container{
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

					MemoryMB:   task.MemoryMB,
					DiskMB:     task.DiskMB,
					CPUWeight:  task.CPUWeight,
					RootFSPath: task.RootFSPath,
					Privileged: task.Privileged,
				}))
			})

			Context("when allocation succeeds", func() {
				BeforeEach(func() {
					client.AllocateContainerReturns(executor.Container{}, nil)
				})

				It("tells the BBS it started the task", func() {
					_, err := cellRep.Perform(work)
					Ω(err).ShouldNot(HaveOccurred())

					Eventually(bbs.StartTaskCallCount).Should(Equal(1))

					_, actualTaskGuid, actualCellID := bbs.StartTaskArgsForCall(0)
					Ω(actualTaskGuid).Should(Equal(task.TaskGuid))
					Ω(actualCellID).Should(Equal("some-cell-id"))
				})

				It("responds successfully before starting the task in the BBS", func() {
					triggerStartChan := make(chan struct{})
					triggerStartCalled := make(chan struct{})

					bbs.StartTaskStub = func(_ lager.Logger, _ string, _ string) (bool, error) {
						<-triggerStartChan
						close(triggerStartCalled)
						return true, nil
					}

					failedWork, err := cellRep.Perform(work)
					Ω(err).ShouldNot(HaveOccurred())
					Ω(failedWork).Should(BeZero())

					Consistently(triggerStartCalled).ShouldNot(BeClosed())
					close(triggerStartChan)
					Eventually(triggerStartCalled).Should(BeClosed())
				})

				Context("when it succeeds marking the task as starting in the BBS", func() {
					BeforeEach(func() {
						bbs.StartTaskReturns(true, nil)
					})

					It("runs the task", func() {
						_, err := cellRep.Perform(work)
						Ω(err).ShouldNot(HaveOccurred())
						Eventually(client.RunContainerCallCount).Should(Equal(1))
						Ω(client.RunContainerArgsForCall(0)).Should(Equal(task.TaskGuid))
					})

					Context("when running the task succeeds", func() {
						BeforeEach(func() {
							client.RunContainerReturns(nil)
						})

						It("responds successfully", func() {
							failedWork, err := cellRep.Perform(work)
							Ω(err).ShouldNot(HaveOccurred(), "note: we don't error")
							Ω(failedWork.Tasks).Should(BeZero())
						})

						It("does not delete the container", func() {
							cellRep.Perform(work)
							Consistently(client.DeleteContainerCallCount).Should(Equal(0))
						})
					})

					Context("when running the task fails", func() {
						BeforeEach(func() {
							client.RunContainerReturns(commonErr)
						})

						It("responds successfully", func() {
							failedWork, err := cellRep.Perform(work)
							Ω(err).ShouldNot(HaveOccurred(), "note: we don't error")
							Ω(failedWork.Tasks).Should(BeZero())
						})

						It("deletes the container", func() {
							cellRep.Perform(work)
							Eventually(client.DeleteContainerCallCount).Should(Equal(1))
							Ω(client.DeleteContainerArgsForCall(0)).Should(Equal(task.TaskGuid))
						})

						It("marks the state as failed in the BBS", func() {
							cellRep.Perform(work)

							Eventually(bbs.FailTaskCallCount).Should(Equal(1))
							_, actualTaskGuid, actualFailureReason := bbs.FailTaskArgsForCall(0)
							Ω(actualTaskGuid).Should(Equal(task.TaskGuid))
							Ω(actualFailureReason).Should(ContainSubstring("failed to run container"))
						})
					})
				})

				Context("when it fails to mark it starting in the BBS", func() {
					BeforeEach(func() {
						bbs.StartTaskReturns(false, commonErr)
					})

					It("responds successfully", func() {
						failedWork, err := cellRep.Perform(work)
						Ω(err).ShouldNot(HaveOccurred(), "note: we don't error")
						Ω(failedWork.Tasks).Should(BeZero())
					})

					It("doesn't run the container", func() {
						cellRep.Perform(work)
						Consistently(client.RunContainerCallCount).Should(Equal(0))
					})

					It("deletes the container", func() {
						cellRep.Perform(work)
						Eventually(client.DeleteContainerCallCount).Should(Equal(1))
						Ω(client.DeleteContainerArgsForCall(0)).Should(Equal(task.TaskGuid))
					})
				})
			})

			Context("when the container fails to allocate", func() {
				BeforeEach(func() {
					client.AllocateContainerReturns(executor.Container{}, commonErr)
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
