package internal_test

import (
	"errors"
	"strconv"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context/fake_evacuation_context"
	"github.com/cloudfoundry-incubator/rep/generator/internal"
	"github.com/cloudfoundry-incubator/rep/generator/internal/fake_internal"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/bbserrors"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EvacuationLrpProcessor", func() {
	Describe("Process", func() {
		const (
			localCellID   = "cell-α"
			evacuationTTL = 1234
		)

		var (
			logger                 *lagertest.TestLogger
			fakeRepBBS             *fake_bbs.FakeRepBBS
			fakeContainerDelegate  *fake_internal.FakeContainerDelegate
			fakeEvacuationReporter *fake_evacuation_context.FakeEvacuationReporter

			lrpProcessor internal.LRPProcessor

			processGuid  string
			desiredLRP   models.DesiredLRP
			index        int
			container    executor.Container
			instanceGuid string

			lrpKey         models.ActualLRPKey
			lrpInstanceKey models.ActualLRPInstanceKey
		)

		BeforeEach(func() {
			logger = lagertest.NewTestLogger("test")

			fakeRepBBS = new(fake_bbs.FakeRepBBS)

			fakeContainerDelegate = &fake_internal.FakeContainerDelegate{}
			fakeEvacuationReporter = &fake_evacuation_context.FakeEvacuationReporter{}
			fakeEvacuationReporter.EvacuatingReturns(true)

			lrpProcessor = internal.NewLRPProcessor(fakeRepBBS, fakeContainerDelegate, localCellID, fakeEvacuationReporter, evacuationTTL)

			processGuid = "process-guid"
			desiredLRP = models.DesiredLRP{
				Domain:      "domain",
				ProcessGuid: processGuid,
				Instances:   1,
				RootFS:      "some-rootfs",
				Action: &models.RunAction{
					Path: "/bin/true",
				},
			}

			instanceGuid = "instance-guid"
			index = 0

			container = executor.Container{
				Guid: rep.LRPContainerGuid(desiredLRP.ProcessGuid, instanceGuid),
				Tags: executor.Tags{
					rep.LifecycleTag:    rep.LRPLifecycle,
					rep.DomainTag:       desiredLRP.Domain,
					rep.ProcessGuidTag:  desiredLRP.ProcessGuid,
					rep.InstanceGuidTag: instanceGuid,
					rep.ProcessIndexTag: strconv.Itoa(index),
				},
			}

			lrpKey = models.NewActualLRPKey(processGuid, index, desiredLRP.Domain)
			lrpInstanceKey = models.NewActualLRPInstanceKey(instanceGuid, localCellID)
		})

		JustBeforeEach(func() {
			lrpProcessor.Process(logger, container)
		})

		Context("when the container is Reserved", func() {
			BeforeEach(func() {
				container.State = executor.StateReserved
			})

			It("evacuates the lrp", func() {
				Ω(fakeRepBBS.EvacuateClaimedActualLRPCallCount()).Should(Equal(1))
				_, actualLRPKey, actualLRPContainerKey := fakeRepBBS.EvacuateClaimedActualLRPArgsForCall(0)
				Ω(actualLRPKey).Should(Equal(lrpKey))
				Ω(actualLRPContainerKey).Should(Equal(lrpInstanceKey))
			})

			Context("when the evacuation returns successfully", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(shared.DeleteContainer, nil)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(container.Guid))
				})
			})

			Context("when the evacuation returns that it failed to unclaim the LRP", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(shared.DeleteContainer, bbserrors.ErrActualLRPCannotBeUnclaimed)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(container.Guid))
				})
			})

			Context("when the evacuation returns some other error", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(shared.DeleteContainer, errors.New("whoops"))
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(container.Guid))
				})
			})
		})

		Context("when the container is Initializing", func() {
			BeforeEach(func() {
				container.State = executor.StateInitializing
			})

			It("evacuates the lrp", func() {
				Ω(fakeRepBBS.EvacuateClaimedActualLRPCallCount()).Should(Equal(1))
				_, actualLRPKey, actualLRPContainerKey := fakeRepBBS.EvacuateClaimedActualLRPArgsForCall(0)
				Ω(actualLRPKey).Should(Equal(lrpKey))
				Ω(actualLRPContainerKey).Should(Equal(lrpInstanceKey))
			})

			Context("when the evacuation returns successfully", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(shared.DeleteContainer, nil)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(container.Guid))
				})
			})

			Context("when the evacuation returns that it failed to unclaim the LRP", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(shared.DeleteContainer, bbserrors.ErrActualLRPCannotBeUnclaimed)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(container.Guid))
				})
			})

			Context("when the evacuation returns some other error", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(shared.DeleteContainer, errors.New("whoops"))
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(container.Guid))
				})
			})
		})

		Context("when the container is Created", func() {
			BeforeEach(func() {
				container.State = executor.StateCreated
			})

			It("evacuates the lrp", func() {
				Ω(fakeRepBBS.EvacuateClaimedActualLRPCallCount()).Should(Equal(1))
				_, actualLRPKey, actualLRPContainerKey := fakeRepBBS.EvacuateClaimedActualLRPArgsForCall(0)
				Ω(actualLRPKey).Should(Equal(lrpKey))
				Ω(actualLRPContainerKey).Should(Equal(lrpInstanceKey))
			})

			Context("when the evacuation returns successfully", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(shared.DeleteContainer, nil)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(container.Guid))
				})
			})

			Context("when the evacuation returns that it failed to unclaim the LRP", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(shared.DeleteContainer, bbserrors.ErrActualLRPCannotBeUnclaimed)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(container.Guid))
				})
			})

			Context("when the evacuation returns some other error", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(shared.DeleteContainer, errors.New("whoops"))
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(container.Guid))
				})
			})
		})

		Context("when the container is Running", func() {
			var lrpNetInfo models.ActualLRPNetInfo

			BeforeEach(func() {
				container.State = executor.StateRunning
				externalIP := "executor-ip"
				container.ExternalIP = externalIP
				container.Ports = []executor.PortMapping{{ContainerPort: 1357, HostPort: 8642}}
				lrpNetInfo = models.NewActualLRPNetInfo(externalIP, []models.PortMapping{{ContainerPort: 1357, HostPort: 8642}})
			})

			It("evacuates the lrp", func() {
				Ω(fakeRepBBS.EvacuateRunningActualLRPCallCount()).Should(Equal(1))
				_, actualLRPKey, actualLRPContainerKey, actualLRPNetInfo, actualTTL := fakeRepBBS.EvacuateRunningActualLRPArgsForCall(0)
				Ω(actualLRPKey).Should(Equal(lrpKey))
				Ω(actualLRPContainerKey).Should(Equal(lrpInstanceKey))
				Ω(actualLRPNetInfo).Should(Equal(lrpNetInfo))
				Ω(actualTTL).Should(Equal(uint64(evacuationTTL)))
			})

			Context("when the evacuation returns successfully", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateRunningActualLRPReturns(shared.KeepContainer, nil)
				})

				It("does not delete the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(0))
				})
			})

			Context("when the evacuation returns that it failed to evacuate the LRP", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateRunningActualLRPReturns(shared.DeleteContainer, bbserrors.ErrActualLRPCannotBeEvacuated)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(container.Guid))
				})
			})

			Context("when the evacuation returns some other error", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateRunningActualLRPReturns(shared.KeepContainer, errors.New("whoops"))
				})

				It("does not delete the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(0))
				})
			})
		})

		Context("when the container is COMPLETED (shutdown)", func() {
			BeforeEach(func() {
				container.State = executor.StateCompleted
				container.RunResult.Stopped = true
			})

			It("evacuates the lrp", func() {
				Ω(fakeRepBBS.EvacuateStoppedActualLRPCallCount()).Should(Equal(1))
				_, actualLRPKey, actualLRPContainerKey := fakeRepBBS.EvacuateStoppedActualLRPArgsForCall(0)
				Ω(actualLRPKey).Should(Equal(lrpKey))
				Ω(actualLRPContainerKey).Should(Equal(lrpInstanceKey))
			})

			Context("when the evacuation returns successfully", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateStoppedActualLRPReturns(shared.DeleteContainer, nil)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(container.Guid))
				})
			})

			Context("when the evacuation returns that it failed to remove the LRP", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateStoppedActualLRPReturns(shared.DeleteContainer, bbserrors.ErrActualLRPCannotBeRemoved)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(container.Guid))
				})
			})

			Context("when the evacuation returns some other error", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateStoppedActualLRPReturns(shared.DeleteContainer, errors.New("whoops"))
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(container.Guid))
				})
			})
		})

		Context("when the container is COMPLETED (crashed)", func() {
			BeforeEach(func() {
				container.State = executor.StateCompleted
				container.RunResult.Stopped = false
				container.RunResult.FailureReason = "crashed"
			})

			It("evacuates the lrp", func() {
				Ω(fakeRepBBS.EvacuateCrashedActualLRPCallCount()).Should(Equal(1))
				_, actualLRPKey, actualLRPContainerKey, reason := fakeRepBBS.EvacuateCrashedActualLRPArgsForCall(0)
				Ω(actualLRPKey).Should(Equal(lrpKey))
				Ω(actualLRPContainerKey).Should(Equal(lrpInstanceKey))
				Ω(reason).Should(Equal("crashed"))
			})

			Context("when the evacuation returns successfully", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateCrashedActualLRPReturns(shared.DeleteContainer, nil)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(container.Guid))
				})
			})

			Context("when the evacuation returns that it failed to remove the LRP", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateCrashedActualLRPReturns(shared.DeleteContainer, bbserrors.ErrActualLRPCannotBeCrashed)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(container.Guid))
				})
			})

			Context("when the evacuation returns some other error", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateCrashedActualLRPReturns(shared.DeleteContainer, errors.New("whoops"))
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(container.Guid))
				})
			})
		})
	})
})
