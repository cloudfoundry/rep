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

			lrpKey          models.ActualLRPKey
			lrpContainerKey models.ActualLRPContainerKey
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
				Stack:       "some-stack",
				Action: &models.RunAction{
					Path: "/bin/true",
				},
			}

			instanceGuid = "instance-guid"
			index = 0

			container = executor.Container{
				Guid: instanceGuid,
				Tags: executor.Tags{
					rep.LifecycleTag:    rep.LRPLifecycle,
					rep.DomainTag:       desiredLRP.Domain,
					rep.ProcessGuidTag:  desiredLRP.ProcessGuid,
					rep.ProcessIndexTag: strconv.Itoa(index),
				},
			}

			lrpKey = models.NewActualLRPKey(processGuid, index, desiredLRP.Domain)
			lrpContainerKey = models.NewActualLRPContainerKey(instanceGuid, localCellID)
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
				Ω(actualLRPContainerKey).Should(Equal(lrpContainerKey))
			})

			Context("when the evacuation returns successfully", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(nil)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(instanceGuid))
				})
			})

			Context("when the evacuation returns that it failed to unclaim the LRP", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(bbserrors.ErrActualLRPCannotBeUnclaimed)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(instanceGuid))
				})
			})

			Context("when the evacuation returns some other error", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(errors.New("whoops"))
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(instanceGuid))
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
				Ω(actualLRPContainerKey).Should(Equal(lrpContainerKey))
			})

			Context("when the evacuation returns successfully", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(nil)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(instanceGuid))
				})
			})

			Context("when the evacuation returns that it failed to unclaim the LRP", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(bbserrors.ErrActualLRPCannotBeUnclaimed)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(instanceGuid))
				})
			})

			Context("when the evacuation returns some other error", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(errors.New("whoops"))
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(instanceGuid))
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
				Ω(actualLRPContainerKey).Should(Equal(lrpContainerKey))
			})

			Context("when the evacuation returns successfully", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(nil)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(instanceGuid))
				})
			})

			Context("when the evacuation returns that it failed to unclaim the LRP", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(bbserrors.ErrActualLRPCannotBeUnclaimed)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(instanceGuid))
				})
			})

			Context("when the evacuation returns some other error", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateClaimedActualLRPReturns(errors.New("whoops"))
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(instanceGuid))
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
				Ω(actualLRPContainerKey).Should(Equal(lrpContainerKey))
				Ω(actualLRPNetInfo).Should(Equal(lrpNetInfo))
				Ω(actualTTL).Should(Equal(uint64(evacuationTTL)))
			})

			Context("when the evacuation returns successfully", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateRunningActualLRPReturns(nil)
				})

				It("does not delete the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(0))
				})
			})

			Context("when the evacuation returns that it failed to evacuate the LRP", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateRunningActualLRPReturns(bbserrors.ErrActualLRPCannotBeEvacuated)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(instanceGuid))
				})
			})

			Context("when the evacuation returns some other error", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateRunningActualLRPReturns(errors.New("whoops"))
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
				Ω(actualLRPContainerKey).Should(Equal(lrpContainerKey))
			})

			Context("when the evacuation returns successfully", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateStoppedActualLRPReturns(nil)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(instanceGuid))
				})
			})

			Context("when the evacuation returns that it failed to remove the LRP", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateStoppedActualLRPReturns(bbserrors.ErrActualLRPCannotBeRemoved)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(instanceGuid))
				})
			})

			Context("when the evacuation returns some other error", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateStoppedActualLRPReturns(errors.New("whoops"))
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(instanceGuid))
				})
			})
		})

		Context("when the container is COMPLETED (crashed)", func() {
			BeforeEach(func() {
				container.State = executor.StateCompleted
				container.RunResult.Stopped = false
			})

			It("evacuates the lrp", func() {
				Ω(fakeRepBBS.EvacuateCrashedActualLRPCallCount()).Should(Equal(1))
				_, actualLRPKey, actualLRPContainerKey := fakeRepBBS.EvacuateCrashedActualLRPArgsForCall(0)
				Ω(actualLRPKey).Should(Equal(lrpKey))
				Ω(actualLRPContainerKey).Should(Equal(lrpContainerKey))
			})

			Context("when the evacuation returns successfully", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateCrashedActualLRPReturns(nil)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(instanceGuid))
				})
			})

			Context("when the evacuation returns that it failed to remove the LRP", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateCrashedActualLRPReturns(bbserrors.ErrActualLRPCannotBeCrashed)
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(instanceGuid))
				})
			})

			Context("when the evacuation returns some other error", func() {
				BeforeEach(func() {
					fakeRepBBS.EvacuateCrashedActualLRPReturns(errors.New("whoops"))
				})

				It("deletes the container", func() {
					Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
					_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
					Ω(actualContainerGuid).Should(Equal(instanceGuid))
				})
			})
		})
	})
})
