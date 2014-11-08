package auction_delegate_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	executor "github.com/cloudfoundry-incubator/executor"
	fake_client "github.com/cloudfoundry-incubator/executor/fakes"
	. "github.com/cloudfoundry-incubator/rep/auction_delegate"
	"github.com/cloudfoundry-incubator/rep/harvester"
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
				resources := executor.ExecutorResources{
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
				client.RemainingResourcesReturns(executor.ExecutorResources{}, clientFetchError)
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
				resources := executor.ExecutorResources{
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
				client.TotalResourcesReturns(executor.ExecutorResources{}, clientFetchError)
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
				containers := []executor.Container{
					executor.Container{
						Guid: "first",
						Tags: executor.Tags{
							ProcessGuidTag:  "the-first-app-guid",
							ProcessIndexTag: "17",
						},
					},
					executor.Container{
						Guid: "third",
						Tags: executor.Tags{
							ProcessGuidTag:  "the-first-app-guid",
							ProcessIndexTag: "92",
						},
					},
				}

				client.ListContainersReturns(containers, nil)
			})

			It("returns the count of the containers that were returned", func() {
				instances, err := delegate.NumInstancesForProcessGuid("the-first-app-guid")
				Ω(err).ShouldNot(HaveOccurred())
				Ω(instances).Should(Equal(2))
			})

			It("filters by process guid", func() {
				_, err := delegate.NumInstancesForProcessGuid("the-first-app-guid")
				Ω(err).ShouldNot(HaveOccurred())
				Ω(client.ListContainersArgsForCall(0)).Should(Equal(executor.Tags{
					ProcessGuidTag: "the-first-app-guid",
				}))
			})
		})

		Context("when the client returns an error", func() {
			BeforeEach(func() {
				client.ListContainersReturns([]executor.Container{}, clientFetchError)
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
				containers := []executor.Container{
					executor.Container{
						Guid: "first",
					},
					executor.Container{
						Guid: "second",
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

			It("filters by process guid and index", func() {
				_, err := delegate.InstanceGuidsForProcessGuidAndIndex("requested-app-guid", 17)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(client.ListContainersArgsForCall(0)).Should(Equal(executor.Tags{
					ProcessGuidTag:  "requested-app-guid",
					ProcessIndexTag: "17",
				}))
			})
		})

		Context("when the client returns an error", func() {
			BeforeEach(func() {
				client.ListContainersReturns([]executor.Container{}, clientFetchError)
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
		var startAuction models.LRPStartAuction
		var expectedRootFS = "docker://docker.com/docker"

		JustBeforeEach(func() {
			reserveErr = delegate.Reserve(startAuction)
		})

		Context("when the client returns a succesful response", func() {
			BeforeEach(func() {
				startAuction = models.LRPStartAuction{
					DesiredLRP: models.DesiredLRP{
						Domain:      "tests",
						RootFSPath:  expectedRootFS,
						ProcessGuid: "process-guid",
						DiskMB:      1024,
						MemoryMB:    2048,
						CPUWeight:   42,
						EnvironmentVariables: []models.EnvironmentVariable{
							{Name: "var1", Value: "val1"},
							{Name: "var2", Value: "val2"},
						},
						Action: &models.ExecutorAction{
							Action: models.DownloadAction{
								From: "http://example.com/something",
								To:   "/something",
							},
						},
						LogGuid: "log-guid",
						Ports: []models.PortMapping{
							{ContainerPort: 8080},
						},
					},

					InstanceGuid: "instance-guid",
					Index:        2,
				}

				client.AllocateContainerReturns(executor.Container{}, nil)
			})

			It("should not return an error", func() {
				Ω(reserveErr).ShouldNot(HaveOccurred())
			})

			It("should allocate a container, passing in the correct data", func() {
				Ω(client.AllocateContainerCallCount()).Should(Equal(1))

				two := 2
				Ω(client.AllocateContainerArgsForCall(0)).Should(Equal(executor.Container{
					Guid: startAuction.InstanceGuid,

					Tags: executor.Tags{
						harvester.LifecycleTag: harvester.LRPLifecycle,
						harvester.DomainTag:    "tests",
						ProcessGuidTag:         startAuction.DesiredLRP.ProcessGuid,
						ProcessIndexTag:        "2",
					},

					MemoryMB:   startAuction.DesiredLRP.MemoryMB,
					DiskMB:     startAuction.DesiredLRP.DiskMB,
					CPUWeight:  startAuction.DesiredLRP.CPUWeight,
					RootFSPath: expectedRootFS,
					Ports:      []executor.PortMapping{{ContainerPort: 8080}},
					Log:        executor.LogConfig{Guid: "log-guid", Index: &two},

					Setup:   startAuction.DesiredLRP.Setup,
					Action:  startAuction.DesiredLRP.Action,
					Monitor: startAuction.DesiredLRP.Monitor,

					Env: []executor.EnvironmentVariable{
						{Name: "CF_INSTANCE_GUID", Value: "instance-guid"},
						{Name: "CF_INSTANCE_INDEX", Value: "2"},
						{Name: "var1", Value: "val1"},
						{Name: "var2", Value: "val2"},
					},
				}))
			})
		})

		Context("when the client returns an error", func() {
			BeforeEach(func() {
				client.AllocateContainerReturns(executor.Container{}, clientFetchError)
			})

			It("should return the error", func() {
				Ω(reserveErr).Should(Equal(clientFetchError))
			})
		})
	})

	Describe("ReleaseReservation", func() {
		var startAuction models.LRPStartAuction
		var releaseErr error

		JustBeforeEach(func() {
			startAuction = models.LRPStartAuction{
				DesiredLRP: models.DesiredLRP{
					Domain:      "tests",
					ProcessGuid: "process-guid",
				},

				InstanceGuid: "instance-guid",
				Index:        2,
			}

			releaseErr = delegate.ReleaseReservation(startAuction)
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
				Ω(client.DeleteContainerArgsForCall(0)).Should(Equal(startAuction.InstanceGuid))
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
		var expectedRootFS = "docker://docker.com/docker"
		var startAuction models.LRPStartAuction
		var err, initializeError, startingErr, runError error
		var expectedLrp models.ActualLRP

		BeforeEach(func() {
			initializeError, startingErr, runError = nil, nil, nil

			startAuction = models.LRPStartAuction{
				DesiredLRP: models.DesiredLRP{
					Domain:      "tests",
					RootFSPath:  expectedRootFS,
					ProcessGuid: "process-guid",
					DiskMB:      1024,
					MemoryMB:    2048,
					Action: &models.ExecutorAction{
						Action: models.DownloadAction{
							From: "http://example.com/something",
							To:   "/something",
						},
					},
					LogGuid: "log-guid",
					Ports: []models.PortMapping{
						{ContainerPort: 8080},
					},
				},

				InstanceGuid: "instance-guid",
				Index:        2,
			}

			expectedLrp = models.ActualLRP{
				ProcessGuid:  startAuction.DesiredLRP.ProcessGuid,
				InstanceGuid: startAuction.InstanceGuid,
				Domain:       startAuction.DesiredLRP.Domain,
				Index:        startAuction.Index,
				ExecutorID:   "some-executor-id",
			}
		})

		JustBeforeEach(func() {
			bbs.ReportActualLRPAsStartingReturns(expectedLrp, startingErr)
			client.RunContainerReturns(runError)
			client.DeleteContainerReturns(nil)

			err = delegate.Run(startAuction)
		})

		Context("when running succeeds", func() {
			It("should not return an error", func() {
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("should mark the instance as STARTING in etcd", func() {
				Ω(bbs.ReportActualLRPAsStartingCallCount()).Should(Equal(1))

				actualProcessGuid, actualInstanceGuid, actualExecutorID, actualDomain, actualIndex := bbs.ReportActualLRPAsStartingArgsForCall(0)
				Ω(actualProcessGuid).Should(Equal(expectedLrp.ProcessGuid))
				Ω(actualInstanceGuid).Should(Equal(expectedLrp.InstanceGuid))
				Ω(actualExecutorID).Should(Equal(expectedLrp.ExecutorID))
				Ω(actualDomain).Should(Equal(expectedLrp.Domain))
				Ω(actualIndex).Should(Equal(expectedLrp.Index))
			})

			It("should attempt to run the correct container", func() {
				Ω(client.RunContainerCallCount()).Should(Equal(1))
				Ω(client.RunContainerArgsForCall(0)).Should(Equal(startAuction.InstanceGuid))
			})

			It("does not attempt to remove the STARTING LRP from etcd", func() {
				Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(0))
			})

			It("should not delete the container", func() {
				Ω(client.DeleteContainerCallCount()).Should(Equal(0))
			})
		})

		Context("when marking the instance as STARTING fails", func() {
			BeforeEach(func() {
				startingErr = errors.New("kaboom")
			})

			It("should bubble up the error", func() {
				Ω(err).Should(Equal(startingErr))
			})

			It("should mark the instance as STARTING in etcd", func() {
				Ω(bbs.ReportActualLRPAsStartingCallCount()).Should(Equal(1))

				actualProcessGuid, actualInstanceGuid, actualExecutorID, actualDomain, actualIndex := bbs.ReportActualLRPAsStartingArgsForCall(0)
				Ω(actualProcessGuid).Should(Equal(expectedLrp.ProcessGuid))
				Ω(actualInstanceGuid).Should(Equal(expectedLrp.InstanceGuid))
				Ω(actualExecutorID).Should(Equal(expectedLrp.ExecutorID))
				Ω(actualDomain).Should(Equal(expectedLrp.Domain))
				Ω(actualIndex).Should(Equal(expectedLrp.Index))
			})

			It("does not attempt to remove the STARTING LRP from etcd", func() {
				Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(0))
			})

			It("should not attempt to run the actions", func() {
				Ω(client.RunContainerCallCount()).Should(Equal(0))
			})

			It("should delete the container", func() {
				Ω(client.DeleteContainerCallCount()).Should(Equal(1))
				allocationGuid := client.DeleteContainerArgsForCall(0)
				Ω(allocationGuid).Should(Equal(startAuction.InstanceGuid))
			})
		})

		Context("when run fails", func() {
			BeforeEach(func() {
				runError = errors.New("Failed to run")
			})

			It("should bubble up the error", func() {
				Ω(err).Should(Equal(runError))
			})

			It("should mark the instance as STARTING in etcd", func() {
				Ω(bbs.ReportActualLRPAsStartingCallCount()).Should(Equal(1))

				actualProcessGuid, actualInstanceGuid, actualExecutorID, actualDomain, actualIndex := bbs.ReportActualLRPAsStartingArgsForCall(0)
				Ω(actualProcessGuid).Should(Equal(expectedLrp.ProcessGuid))
				Ω(actualInstanceGuid).Should(Equal(expectedLrp.InstanceGuid))
				Ω(actualExecutorID).Should(Equal(expectedLrp.ExecutorID))
				Ω(actualDomain).Should(Equal(expectedLrp.Domain))
				Ω(actualIndex).Should(Equal(expectedLrp.Index))
			})

			It("should remove the STARTING LRP from etcd", func() {
				Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(1))
				Ω(bbs.RemoveActualLRPArgsForCall(0).InstanceGuid).Should(Equal(startAuction.InstanceGuid))
			})

			It("should delete the container", func() {
				Ω(client.DeleteContainerCallCount()).Should(Equal(1))
				allocationGuid := client.DeleteContainerArgsForCall(0)
				Ω(allocationGuid).Should(Equal(startAuction.InstanceGuid))
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
