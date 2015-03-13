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
	. "github.com/onsi/gomega/gbytes"
)

var _ = Describe("OrdinaryLRPProcessor", func() {
	const expectedCellID = "cell-id"

	var processor internal.LRPProcessor
	var logger *lagertest.TestLogger
	var bbs *fake_bbs.FakeRepBBS
	var containerDelegate *fake_internal.FakeContainerDelegate
	var evacuationReporter *fake_evacuation_context.FakeEvacuationReporter

	BeforeEach(func() {
		bbs = new(fake_bbs.FakeRepBBS)
		containerDelegate = new(fake_internal.FakeContainerDelegate)
		evacuationReporter = &fake_evacuation_context.FakeEvacuationReporter{}
		evacuationReporter.EvacuatingReturns(false)
		processor = internal.NewLRPProcessor(bbs, containerDelegate, expectedCellID, evacuationReporter, 124)
		logger = lagertest.NewTestLogger("test")
	})

	Describe("Process", func() {
		const sessionPrefix = "test.ordinary-lrp-processor."

		var expectedLrpKey models.ActualLRPKey
		var expectedInstanceKey models.ActualLRPInstanceKey
		var expectedNetInfo models.ActualLRPNetInfo
		var expectedSessionName string

		BeforeEach(func() {
			expectedLrpKey = models.NewActualLRPKey("process-guid", 2, "domain")
			expectedInstanceKey = models.NewActualLRPInstanceKey("instance-guid", "cell-id")
			expectedNetInfo = models.NewActualLRPNetInfo("1.2.3.4", []models.PortMapping{{ContainerPort: 8080, HostPort: 61999}})
		})

		Context("when given an LRP container", func() {
			var container executor.Container

			BeforeEach(func() {
				container = newLRPContainer(expectedLrpKey, expectedInstanceKey, expectedNetInfo)
			})

			JustBeforeEach(func() {
				processor.Process(logger, container)
			})

			Context("and the container is INVALID", func() {
				BeforeEach(func() {
					expectedSessionName = sessionPrefix + "process-invalid-container"
					container.State = executor.StateInvalid
				})

				It("logs an error", func() {
					Ω(logger).Should(Say(expectedSessionName))
				})
			})

			Context("and the container is RESERVED", func() {
				BeforeEach(func() {
					expectedSessionName = sessionPrefix + "process-reserved-container"
					container.State = executor.StateReserved
				})

				It("claims the actualLRP in the bbs", func() {
					Ω(bbs.ClaimActualLRPCallCount()).Should(Equal(1))
					bbsLogger, lrpKey, instanceKey := bbs.ClaimActualLRPArgsForCall(0)
					Ω(lrpKey).Should(Equal(expectedLrpKey))
					Ω(instanceKey).Should(Equal(expectedInstanceKey))
					Ω(bbsLogger.SessionName()).Should(Equal(expectedSessionName))
				})

				Context("when claiming fails because ErrActualLRPCannotBeClaimed", func() {
					BeforeEach(func() {
						bbs.ClaimActualLRPReturns(bbserrors.ErrActualLRPCannotBeClaimed)
					})

					It("deletes the container", func() {
						Ω(containerDelegate.DeleteContainerCallCount()).Should(Equal(1))
						delegateLogger, containerGuid := containerDelegate.DeleteContainerArgsForCall(0)
						Ω(containerGuid).Should(Equal(container.Guid))
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
						Ω(containerGuid).Should(Equal(container.Guid))
						Ω(delegateLogger.SessionName()).Should(Equal(expectedSessionName))
					})

					Context("when running fails", func() {
						BeforeEach(func() {
							containerDelegate.RunContainerReturns(false)
						})

						It("removes the actual LRP", func() {
							Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(1))
							bbsLogger, lrpKey, instanceKey := bbs.RemoveActualLRPArgsForCall(0)
							Ω(lrpKey).Should(Equal(expectedLrpKey))
							Ω(instanceKey).Should(Equal(expectedInstanceKey))
							Ω(bbsLogger.SessionName()).Should(Equal(expectedSessionName))
						})
					})
				})

				var itClaimsTheLRPOrDeletesTheContainer = func(expectedSessionName string) {
					It("claims the lrp", func() {
						Ω(bbs.ClaimActualLRPCallCount()).Should(Equal(1))
						bbsLogger, lrpKey, instanceKey := bbs.ClaimActualLRPArgsForCall(0)
						Ω(lrpKey).Should(Equal(expectedLrpKey))
						Ω(instanceKey).Should(Equal(expectedInstanceKey))
						Ω(bbsLogger.SessionName()).Should(Equal(expectedSessionName))
					})

					Context("when the claim fails because ErrActualLRPCannotBeClaimed", func() {
						BeforeEach(func() {
							bbs.ClaimActualLRPReturns(bbserrors.ErrActualLRPCannotBeClaimed)
						})

						It("deletes the container", func() {
							Ω(containerDelegate.DeleteContainerCallCount()).Should(Equal(1))
							delegateLogger, containerGuid := containerDelegate.DeleteContainerArgsForCall(0)
							Ω(containerGuid).Should(Equal(container.Guid))
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
						container.Ports = []executor.PortMapping{{ContainerPort: 8080, HostPort: 61999}}
					})

					It("starts the lrp in the bbs", func() {
						Ω(bbs.StartActualLRPCallCount()).Should(Equal(1))
						bbsLogger, lrpKey, instanceKey, netInfo := bbs.StartActualLRPArgsForCall(0)
						Ω(lrpKey).Should(Equal(expectedLrpKey))
						Ω(instanceKey).Should(Equal(expectedInstanceKey))
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
							Ω(containerGuid).Should(Equal(container.Guid))
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

					Context("and the container was requested to stop", func() {
						BeforeEach(func() {
							container.RunResult.Stopped = true
						})

						It("removes the actual LRP", func() {
							Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(1))
							bbsLogger, lrpKey, instanceKey := bbs.RemoveActualLRPArgsForCall(0)
							Ω(lrpKey).Should(Equal(expectedLrpKey))
							Ω(instanceKey).Should(Equal(expectedInstanceKey))
							Ω(bbsLogger.SessionName()).Should(Equal(expectedSessionName))
						})

						Context("when the removal succeeds", func() {
							It("deletes the container", func() {
								Ω(containerDelegate.DeleteContainerCallCount()).Should(Equal(1))
								delegateLogger, containerGuid := containerDelegate.DeleteContainerArgsForCall(0)
								Ω(containerGuid).Should(Equal(container.Guid))
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
								Ω(containerGuid).Should(Equal(container.Guid))
								Ω(delegateLogger.SessionName()).Should(Equal(expectedSessionName))
							})
						})
					})

					Context("and the container was not requested to stop", func() {
						BeforeEach(func() {
							container.RunResult.Stopped = false
						})

						It("crashes the actual LRP", func() {
							Ω(bbs.CrashActualLRPCallCount()).Should(Equal(1))
							bbsLogger, lrpKey, instanceKey := bbs.CrashActualLRPArgsForCall(0)
							Ω(lrpKey).Should(Equal(expectedLrpKey))
							Ω(instanceKey).Should(Equal(expectedInstanceKey))
							Ω(bbsLogger.SessionName()).Should(Equal(expectedSessionName))
						})

						It("deletes the container", func() {
							Ω(containerDelegate.DeleteContainerCallCount()).Should(Equal(1))
							delegateLogger, containerGuid := containerDelegate.DeleteContainerArgsForCall(0)
							Ω(containerGuid).Should(Equal(container.Guid))
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
})

func newLRPContainer(lrpKey models.ActualLRPKey, instanceKey models.ActualLRPInstanceKey, netInfo models.ActualLRPNetInfo) executor.Container {
	ports := []executor.PortMapping{}
	for _, portMap := range netInfo.Ports {
		ports = append(ports, executor.PortMapping{
			ContainerPort: portMap.ContainerPort,
			HostPort:      portMap.HostPort,
		})
	}

	return executor.Container{
		Guid:       rep.LRPContainerGuid(lrpKey.ProcessGuid, instanceKey.InstanceGuid),
		Action:     &models.RunAction{Path: "true"},
		ExternalIP: netInfo.Address,
		Ports:      ports,
		Tags: executor.Tags{
			rep.ProcessGuidTag:  lrpKey.ProcessGuid,
			rep.InstanceGuidTag: instanceKey.InstanceGuid,
			rep.ProcessIndexTag: strconv.Itoa(lrpKey.Index),
			rep.DomainTag:       lrpKey.Domain,
		},
	}
}
