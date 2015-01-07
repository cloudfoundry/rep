package snapshot_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep/snapshot"
	"github.com/cloudfoundry-incubator/rep/snapshot/fake_snapshot"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/bbserrors"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
)

var _ = Describe("LrpProcessor", func() {
	var processor snapshot.LRPProcessor
	var logger *lagertest.TestLogger
	var bbs *fake_bbs.FakeRepBBS
	var containerDelegate *fake_snapshot.FakeContainerDelegate

	BeforeEach(func() {
		bbs = new(fake_bbs.FakeRepBBS)
		containerDelegate = new(fake_snapshot.FakeContainerDelegate)
		processor = snapshot.NewLRPProcessor(bbs, containerDelegate)
		logger = lagertest.NewTestLogger("test")
	})

	Describe("Process", func() {
		const sessionPrefix = "test.lrp-processor."

		var expectedLrpKey models.ActualLRPKey
		var expectedContainerKey models.ActualLRPContainerKey
		var expectedNetInfo models.ActualLRPNetInfo
		var expectedSessionName string

		BeforeEach(func() {
			expectedLrpKey = models.NewActualLRPKey("process-guid", 2, "domain")
			expectedContainerKey = models.NewActualLRPContainerKey("instance-guid", "cell-id")
			expectedNetInfo = models.NewActualLRPNetInfo("1.2.3.4", []models.PortMapping{{ContainerPort: 8080, HostPort: 99999}})
		})

		Context("when given an invalid snapshot", func() {
			BeforeEach(func() {
				snap := snapshot.NewLRPSnapshot(nil, nil)
				processor.Process(logger, snap)
			})

			It("logs an error", func() {
				Ω(logger).Should(Say(sessionPrefix + "invalid-snapshot"))
			})
		})

		Context("when given a snapshot containing only an LRP", func() {
			JustBeforeEach(func() {
				lrp := snapshot.NewLRP(expectedLrpKey, expectedContainerKey)
				snap := snapshot.NewLRPSnapshot(lrp, nil)
				processor.Process(logger, snap)
			})

			It("removes the actualLRP from the BBS", func() {
				Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(1))

				lrpKey, containerKey, bbsLogger := bbs.RemoveActualLRPArgsForCall(0)
				Ω(lrpKey).Should(Equal(expectedLrpKey))
				Ω(containerKey).Should(Equal(expectedContainerKey))
				Ω(bbsLogger.SessionName()).Should(Equal(sessionPrefix + "process-missing-container"))
			})
		})

		Context("when given a snapshot containing both an LRP and a Container", func() {
			var container *executor.Container

			BeforeEach(func() {
				container = &executor.Container{
					Guid: expectedContainerKey.InstanceGuid,
				}
			})

			JustBeforeEach(func() {
				lrp := snapshot.NewLRP(expectedLrpKey, expectedContainerKey)
				snap := snapshot.NewLRPSnapshot(lrp, container)
				processor.Process(logger, snap)
			})

			Context("and the container is RESERVED", func() {
				BeforeEach(func() {
					expectedSessionName = sessionPrefix + "process-reserved-container"
					container.State = executor.StateReserved
				})

				It("claims the actualLRP in the bbs", func() {
					Ω(bbs.ClaimActualLRPCallCount()).Should(Equal(1))
					lrpKey, containerKey, bbsLogger := bbs.ClaimActualLRPArgsForCall(0)
					Ω(lrpKey).Should(Equal(expectedLrpKey))
					Ω(containerKey).Should(Equal(expectedContainerKey))
					Ω(bbsLogger.SessionName()).Should(Equal(expectedSessionName))
				})

				Context("when claiming fails because ErrActualLRPCannotBeClaimed", func() {
					BeforeEach(func() {
						bbs.ClaimActualLRPReturns(bbserrors.ErrActualLRPCannotBeClaimed)
					})

					It("deletes the container", func() {
						Ω(containerDelegate.DeleteContainerCallCount()).Should(Equal(1))
						delegateLogger, containerGuid := containerDelegate.DeleteContainerArgsForCall(0)
						Ω(containerGuid).Should(Equal(expectedContainerKey.InstanceGuid))
						Ω(delegateLogger.SessionName()).Should(Equal(expectedSessionName))
					})

					It("does not try to run the container", func() {
						Ω(containerDelegate.RunContainerCallCount()).Should(Equal(0))
					})
				})

				Context("when claiming fails for an unknown reason", func() {
					BeforeEach(func() {
						bbs.ClaimActualLRPReturns(errors.New("boom"))
					})

					It("does not delete the container", func() {
						Ω(containerDelegate.DeleteContainerCallCount()).Should(Equal(0))
					})

					It("does not try to run the container", func() {
						Ω(containerDelegate.RunContainerCallCount()).Should(Equal(0))
					})
				})

				Context("when claiming succeeds", func() {
					It("runs the container", func() {
						Ω(containerDelegate.RunContainerCallCount()).Should(Equal(1))
						delegateLogger, containerGuid := containerDelegate.RunContainerArgsForCall(0)
						Ω(containerGuid).Should(Equal(expectedContainerKey.InstanceGuid))
						Ω(delegateLogger.SessionName()).Should(Equal(expectedSessionName))
					})

					Context("when running fails", func() {
						BeforeEach(func() {
							containerDelegate.RunContainerReturns(false)
						})

						It("removes the actual LRP", func() {
							Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(1))
							lrpKey, containerKey, bbsLogger := bbs.RemoveActualLRPArgsForCall(0)
							Ω(lrpKey).Should(Equal(expectedLrpKey))
							Ω(containerKey).Should(Equal(expectedContainerKey))
							Ω(bbsLogger.SessionName()).Should(Equal(expectedSessionName))
						})
					})
				})
			})

			var itClaimsTheLRPOrDeletesTheContainer = func(expectedSessionName string) {
				It("claims the lrp", func() {
					Ω(bbs.ClaimActualLRPCallCount()).Should(Equal(1))
					lrpKey, containerKey, bbsLogger := bbs.ClaimActualLRPArgsForCall(0)
					Ω(lrpKey).Should(Equal(expectedLrpKey))
					Ω(containerKey).Should(Equal(expectedContainerKey))
					Ω(bbsLogger.SessionName()).Should(Equal(expectedSessionName))
				})

				Context("when the claim fails because ErrActualLRPCannotBeClaimed", func() {
					BeforeEach(func() {
						bbs.ClaimActualLRPReturns(bbserrors.ErrActualLRPCannotBeClaimed)
					})

					It("deletes the container", func() {
						Ω(containerDelegate.DeleteContainerCallCount()).Should(Equal(1))
						delegateLogger, containerGuid := containerDelegate.DeleteContainerArgsForCall(0)
						Ω(containerGuid).Should(Equal(expectedContainerKey.InstanceGuid))
						Ω(delegateLogger.SessionName()).Should(Equal(expectedSessionName))
					})
				})

				Context("when the claim fails for an unknown reason", func() {
					BeforeEach(func() {
						bbs.ClaimActualLRPReturns(errors.New("boom"))
					})

					It("does not stop or delete the container", func() {
						Ω(containerDelegate.StopContainerCallCount()).Should(Equal(0))
						Ω(containerDelegate.DeleteContainerCallCount()).Should(Equal(0))
					})
				})
			}

			Context("and the container is INITIALIZING", func() {
				BeforeEach(func() {
					container.State = executor.StateInitializing
				})

				itClaimsTheLRPOrDeletesTheContainer(sessionPrefix + "process-initializing-container")
			})

			Context("and the container is CREATED", func() {
				BeforeEach(func() {
					container.State = executor.StateCreated
				})

				itClaimsTheLRPOrDeletesTheContainer(sessionPrefix + "process-created-container")
			})

			Context("and the container is RUNNING", func() {
				BeforeEach(func() {
					expectedSessionName = sessionPrefix + "process-running-container"
					container.State = executor.StateRunning
					container.ExternalIP = "1.2.3.4"
					container.Ports = []executor.PortMapping{{ContainerPort: 8080, HostPort: 99999}}
				})

				It("starts the lrp in the bbs", func() {
					Ω(bbs.StartActualLRPCallCount()).Should(Equal(1))
					lrpKey, containerKey, netInfo, bbsLogger := bbs.StartActualLRPArgsForCall(0)
					Ω(lrpKey).Should(Equal(expectedLrpKey))
					Ω(containerKey).Should(Equal(expectedContainerKey))
					Ω(netInfo).Should(Equal(expectedNetInfo))
					Ω(bbsLogger.SessionName()).Should(Equal(expectedSessionName))
				})

				Context("when starting fails because ErrActualLRPCannotBeStarted", func() {
					BeforeEach(func() {
						bbs.StartActualLRPReturns(bbserrors.ErrActualLRPCannotBeStarted)
					})

					It("stops the container", func() {
						Ω(containerDelegate.StopContainerCallCount()).Should(Equal(1))
						delegateLogger, containerGuid := containerDelegate.StopContainerArgsForCall(0)
						Ω(containerGuid).Should(Equal(expectedContainerKey.InstanceGuid))
						Ω(delegateLogger.SessionName()).Should(Equal(expectedSessionName))
					})
				})

				Context("when starting fails for an unknown reason", func() {
					BeforeEach(func() {
						bbs.StartActualLRPReturns(errors.New("boom"))
					})

					It("does not stop or delete the container", func() {
						Ω(containerDelegate.StopContainerCallCount()).Should(Equal(0))
						Ω(containerDelegate.DeleteContainerCallCount()).Should(Equal(0))
					})
				})
			})

			Context("and the container is COMPLETED", func() {
				BeforeEach(func() {
					expectedSessionName = sessionPrefix + "process-completed-container"
					container.State = executor.StateCompleted
				})

				It("removes the actual LRP", func() {
					Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(1))
					lrpKey, containerKey, bbsLogger := bbs.RemoveActualLRPArgsForCall(0)
					Ω(lrpKey).Should(Equal(expectedLrpKey))
					Ω(containerKey).Should(Equal(expectedContainerKey))
					Ω(bbsLogger.SessionName()).Should(Equal(expectedSessionName))
				})

				Context("when the removal succeeds", func() {
					It("deletes the container", func() {
						Ω(containerDelegate.DeleteContainerCallCount()).Should(Equal(1))
						delegateLogger, containerGuid := containerDelegate.DeleteContainerArgsForCall(0)
						Ω(containerGuid).Should(Equal(expectedContainerKey.InstanceGuid))
						Ω(delegateLogger.SessionName()).Should(Equal(expectedSessionName))
					})
				})

				Context("when the removal fails", func() {
					BeforeEach(func() {
						bbs.RemoveActualLRPReturns(errors.New("whoops"))
					})

					It("deletes the container", func() {
						Ω(containerDelegate.DeleteContainerCallCount()).Should(Equal(1))
						delegateLogger, containerGuid := containerDelegate.DeleteContainerArgsForCall(0)
						Ω(containerGuid).Should(Equal(expectedContainerKey.InstanceGuid))
						Ω(delegateLogger.SessionName()).Should(Equal(expectedSessionName))
					})
				})
			})

			Context("and the container is in an invalid state", func() {
				BeforeEach(func() {
					container.State = executor.StateInvalid
				})

				It("logs the container as a warning", func() {
					Ω(logger).Should(Say(sessionPrefix + "process-invalid-container.not-processing-container-in-invalid-state"))
				})
			})
		})
	})
})
