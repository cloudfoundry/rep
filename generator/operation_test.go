package generator_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/generator"
	"github.com/cloudfoundry-incubator/rep/generator/internal"
	"github.com/cloudfoundry-incubator/rep/generator/internal/fake_internal"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
)

var _ = Describe("Operation", func() {
	Describe("ResidualInstanceLRPOperation", func() {
		var (
			containerDelegate    *fake_internal.FakeContainerDelegate
			residualLRPOperation *generator.ResidualInstanceLRPOperation
			lrpKey               models.ActualLRPKey
			containerKey         models.ActualLRPContainerKey
		)

		BeforeEach(func() {
			lrpKey = models.NewActualLRPKey("the-process-guid", 0, "the-domain")
			containerKey = models.NewActualLRPContainerKey("the-instance-guid", "the-cell-id")
			containerDelegate = new(fake_internal.FakeContainerDelegate)
			residualLRPOperation = generator.NewResidualInstanceLRPOperation(logger, fakeBBS, containerDelegate, lrpKey, containerKey)
		})

		Describe("Key", func() {
			It("returns the InstanceGuid", func() {
				Ω(residualLRPOperation.Key()).Should(Equal("the-instance-guid"))
			})
		})

		Describe("Execute", func() {
			const sessionName = "test.executing-residual-instance-lrp-operation"

			JustBeforeEach(func() {
				residualLRPOperation.Execute()
			})

			It("checks whether the container exists", func() {
				Ω(containerDelegate.GetContainerCallCount()).Should(Equal(1))
				containerDelegateLogger, containerGuid := containerDelegate.GetContainerArgsForCall(0)
				Ω(containerGuid).Should(Equal(containerKey.InstanceGuid))
				Ω(containerDelegateLogger.SessionName()).Should(Equal(sessionName))
			})

			It("logs its execution lifecycle", func() {
				Ω(logger).Should(Say(sessionName + ".starting"))
				Ω(logger).Should(Say(sessionName + ".finished"))
			})

			Context("when the container does not exist", func() {
				BeforeEach(func() {
					containerDelegate.GetContainerReturns(executor.Container{}, false)
				})

				It("removes the actualLRP", func() {
					Ω(fakeBBS.RemoveActualLRPCallCount()).Should(Equal(1))
					bbsLogger, actualLRPKey, actualLRPContainerKey := fakeBBS.RemoveActualLRPArgsForCall(0)
					Ω(actualLRPKey).Should(Equal(lrpKey))
					Ω(actualLRPContainerKey).Should(Equal(containerKey))
					Ω(bbsLogger.SessionName()).Should(Equal(sessionName))
				})
			})

			Context("when the container exists", func() {
				BeforeEach(func() {
					containerDelegate.GetContainerReturns(executor.Container{}, true)
				})

				It("does not remove the actualLRP", func() {
					Ω(fakeBBS.RemoveActualLRPCallCount()).Should(Equal(0))
				})

				It("logs that it skipped the operation because the container was found", func() {
					Ω(logger).Should(Say(sessionName + ".skipped-because-container-exists"))
				})
			})
		})
	})

	Describe("ResidualEvacuatingLRPOperation", func() {
		var (
			containerDelegate              *fake_internal.FakeContainerDelegate
			residualEvacuatingLRPOperation *generator.ResidualEvacuatingLRPOperation
			instanceGuid                   string
			lrpKey                         models.ActualLRPKey
			containerKey                   models.ActualLRPContainerKey
		)

		BeforeEach(func() {
			instanceGuid = "the-instance-guid"
			lrpKey = models.NewActualLRPKey("the-process-guid", 0, "the-domain")
			containerKey = models.NewActualLRPContainerKey(instanceGuid, "the-cell-id")
			containerDelegate = new(fake_internal.FakeContainerDelegate)
			residualEvacuatingLRPOperation = generator.NewResidualEvacuatingLRPOperation(logger, fakeBBS, containerDelegate, lrpKey, containerKey)
		})

		Describe("Key", func() {
			It("returns the InstanceGuid", func() {
				Ω(residualEvacuatingLRPOperation.Key()).Should(Equal(instanceGuid))
			})
		})

		Describe("Execute", func() {
			const sessionName = "test.executing-residual-evacuating-lrp-operation"

			JustBeforeEach(func() {
				residualEvacuatingLRPOperation.Execute()
			})

			It("checks whether the container exists", func() {
				Ω(containerDelegate.GetContainerCallCount()).Should(Equal(1))
				containerDelegateLogger, containerGuid := containerDelegate.GetContainerArgsForCall(0)
				Ω(containerGuid).Should(Equal(containerKey.InstanceGuid))
				Ω(containerDelegateLogger.SessionName()).Should(Equal(sessionName))
			})

			It("logs its execution lifecycle", func() {
				Ω(logger).Should(Say(sessionName + ".starting"))
				Ω(logger).Should(Say(sessionName + ".finished"))
			})

			Context("when the container does not exist", func() {
				BeforeEach(func() {
					containerDelegate.GetContainerReturns(executor.Container{}, false)
				})

				It("removes the actualLRP", func() {
					Ω(fakeBBS.RemoveEvacuatingActualLRPCallCount()).Should(Equal(1))
					bbsLogger, actualLRPKey, actualLRPContainerKey := fakeBBS.RemoveEvacuatingActualLRPArgsForCall(0)
					Ω(actualLRPKey).Should(Equal(lrpKey))
					Ω(actualLRPContainerKey).Should(Equal(containerKey))
					Ω(bbsLogger.SessionName()).Should(Equal(sessionName))
				})
			})

			Context("when the container exists", func() {
				BeforeEach(func() {
					containerDelegate.GetContainerReturns(executor.Container{}, true)
				})

				It("does not remove the actualLRP", func() {
					Ω(fakeBBS.RemoveEvacuatingActualLRPCallCount()).Should(Equal(0))
				})

				It("logs that it skipped the operation because the container was found", func() {
					Ω(logger).Should(Say(sessionName + ".skipped-because-container-exists"))
				})
			})
		})
	})

	Describe("ResidualJointLRPOperation", func() {
		var (
			containerDelegate         *fake_internal.FakeContainerDelegate
			residualJointLRPOperation *generator.ResidualJointLRPOperation
			instanceGuid              string
			lrpKey                    models.ActualLRPKey
			containerKey              models.ActualLRPContainerKey
		)

		BeforeEach(func() {
			instanceGuid = "the-instance-guid"
			lrpKey = models.NewActualLRPKey("the-process-guid", 0, "the-domain")
			containerKey = models.NewActualLRPContainerKey(instanceGuid, "the-cell-id")
			containerDelegate = new(fake_internal.FakeContainerDelegate)
			residualJointLRPOperation = generator.NewResidualJointLRPOperation(logger, fakeBBS, containerDelegate, lrpKey, containerKey)
		})

		Describe("Key", func() {
			It("returns the InstanceGuid", func() {
				Ω(residualJointLRPOperation.Key()).Should(Equal(instanceGuid))
			})
		})

		Describe("Execute", func() {
			const sessionName = "test.executing-residual-joint-lrp-operation"

			JustBeforeEach(func() {
				residualJointLRPOperation.Execute()
			})

			It("checks whether the container exists", func() {
				Ω(containerDelegate.GetContainerCallCount()).Should(Equal(1))
				containerDelegateLogger, containerGuid := containerDelegate.GetContainerArgsForCall(0)
				Ω(containerGuid).Should(Equal(containerKey.InstanceGuid))
				Ω(containerDelegateLogger.SessionName()).Should(Equal(sessionName))
			})

			It("logs its execution lifecycle", func() {
				Ω(logger).Should(Say(sessionName + ".starting"))
				Ω(logger).Should(Say(sessionName + ".finished"))
			})

			Context("when the container does not exist", func() {
				BeforeEach(func() {
					containerDelegate.GetContainerReturns(executor.Container{}, false)
				})

				It("removes the instance actualLRP", func() {
					Ω(fakeBBS.RemoveActualLRPCallCount()).Should(Equal(1))
					bbsLogger, actualLRPKey, actualLRPContainerKey := fakeBBS.RemoveActualLRPArgsForCall(0)
					Ω(actualLRPKey).Should(Equal(lrpKey))
					Ω(actualLRPContainerKey).Should(Equal(containerKey))
					Ω(bbsLogger.SessionName()).Should(Equal(sessionName))
				})

				It("removes the evacuating actualLRP", func() {
					Ω(fakeBBS.RemoveEvacuatingActualLRPCallCount()).Should(Equal(1))
					bbsLogger, actualLRPKey, actualLRPContainerKey := fakeBBS.RemoveEvacuatingActualLRPArgsForCall(0)
					Ω(actualLRPKey).Should(Equal(lrpKey))
					Ω(actualLRPContainerKey).Should(Equal(containerKey))
					Ω(bbsLogger.SessionName()).Should(Equal(sessionName))
				})
			})

			Context("when the container exists", func() {
				BeforeEach(func() {
					containerDelegate.GetContainerReturns(executor.Container{}, true)
				})

				It("does not remove either actualLRP", func() {
					Ω(fakeBBS.RemoveActualLRPCallCount()).Should(Equal(0))
					Ω(fakeBBS.RemoveEvacuatingActualLRPCallCount()).Should(Equal(0))
				})

				It("logs that it skipped the operation because the container was found", func() {
					Ω(logger).Should(Say(sessionName + ".skipped-because-container-exists"))
				})
			})
		})
	})

	Describe("ResidualTaskOperation", func() {
		var (
			containerDelegate     *fake_internal.FakeContainerDelegate
			residualTaskOperation *generator.ResidualTaskOperation
			taskGuid              string
		)

		BeforeEach(func() {
			taskGuid = "the-task-guid"
			containerDelegate = new(fake_internal.FakeContainerDelegate)
			residualTaskOperation = generator.NewResidualTaskOperation(logger, fakeBBS, containerDelegate, taskGuid)
		})

		Describe("Key", func() {
			It("returns the TaskGuid", func() {
				Ω(residualTaskOperation.Key()).Should(Equal("the-task-guid"))
			})
		})

		Describe("Execute", func() {
			const sessionName = "test.executing-residual-task-operation"

			JustBeforeEach(func() {
				residualTaskOperation.Execute()
			})

			It("checks whether the container exists", func() {
				Ω(containerDelegate.GetContainerCallCount()).Should(Equal(1))
				containerDelegateLogger, containerGuid := containerDelegate.GetContainerArgsForCall(0)
				Ω(containerGuid).Should(Equal("the-task-guid"))
				Ω(containerDelegateLogger.SessionName()).Should(Equal(sessionName))
			})

			It("logs its execution lifecycle", func() {
				Ω(logger).Should(Say(sessionName + ".starting"))
				Ω(logger).Should(Say(sessionName + ".finished"))
			})

			Context("when the container does not exist", func() {
				BeforeEach(func() {
					containerDelegate.GetContainerReturns(executor.Container{}, false)
				})

				It("fails the task", func() {
					Ω(fakeBBS.FailTaskCallCount()).Should(Equal(1))
					actualLogger, actualTaskGuid, actualFailureReason := fakeBBS.FailTaskArgsForCall(0)
					Ω(actualLogger.SessionName()).Should(Equal(sessionName))
					Ω(actualTaskGuid).Should(Equal(taskGuid))
					Ω(actualFailureReason).Should(Equal(internal.TaskCompletionReasonMissingContainer))
				})

				Context("when failing the task fails", func() {
					BeforeEach(func() {
						fakeBBS.FailTaskReturns(errors.New("failed"))
					})

					It("logs the failure", func() {
						Ω(logger).Should(Say(sessionName + ".failed-to-fail-task"))
					})
				})
			})

			Context("when the container exists", func() {
				BeforeEach(func() {
					containerDelegate.GetContainerReturns(executor.Container{}, true)
				})

				It("does not fail the task", func() {
					Ω(fakeBBS.FailTaskCallCount()).Should(Equal(0))
				})

				It("logs that it skipped the operation because the container was found", func() {
					Ω(logger).Should(Say(sessionName + ".skipped-because-container-exists"))
				})
			})
		})
	})

	Describe("ContainerOperation", func() {
		var (
			containerDelegate  *fake_internal.FakeContainerDelegate
			lrpProcessor       *fake_internal.FakeLRPProcessor
			taskProcessor      *fake_internal.FakeTaskProcessor
			containerOperation *generator.ContainerOperation
			guid               string
		)

		BeforeEach(func() {
			containerDelegate = new(fake_internal.FakeContainerDelegate)
			lrpProcessor = new(fake_internal.FakeLRPProcessor)
			taskProcessor = new(fake_internal.FakeTaskProcessor)
			guid = "the-guid"
			containerOperation = generator.NewContainerOperation(logger, lrpProcessor, taskProcessor, containerDelegate, guid)
		})

		Describe("Key", func() {
			It("returns the Guid", func() {
				Ω(containerOperation.Key()).Should(Equal("the-guid"))
			})
		})

		Describe("Execute", func() {
			const sessionName = "test.executing-container-operation"

			JustBeforeEach(func() {
				containerOperation.Execute()
			})

			It("checks whether the container exists", func() {
				Ω(containerDelegate.GetContainerCallCount()).Should(Equal(1))
				containerDelegateLogger, containerGuid := containerDelegate.GetContainerArgsForCall(0)
				Ω(containerGuid).Should(Equal(guid))
				Ω(containerDelegateLogger.SessionName()).Should(Equal(sessionName))
			})

			It("logs its execution lifecycle", func() {
				Ω(logger).Should(Say(sessionName + ".starting"))
				Ω(logger).Should(Say(sessionName + ".finished"))
			})

			Context("when the container does not exist", func() {
				BeforeEach(func() {
					containerDelegate.GetContainerReturns(executor.Container{}, false)
				})

				It("logs that it skipped the operation because the container was found", func() {
					Ω(logger).Should(Say(sessionName + ".skipped-because-container-does-not-exist"))
				})

				It("does not farm the container out to any processor", func() {
					Ω(lrpProcessor.ProcessCallCount()).Should(Equal(0))
					Ω(taskProcessor.ProcessCallCount()).Should(Equal(0))
				})
			})

			Context("when the container exists", func() {
				var (
					container executor.Container
				)

				BeforeEach(func() {
					containerDelegate.GetContainerReturns(executor.Container{}, true)
				})

				Context("when the container has an LRP lifecycle tag", func() {
					BeforeEach(func() {
						container = executor.Container{
							Tags: executor.Tags{
								rep.LifecycleTag: rep.LRPLifecycle,
							},
						}
						containerDelegate.GetContainerReturns(container, true)
					})

					It("farms the container out to only the lrp processor", func() {
						Ω(lrpProcessor.ProcessCallCount()).Should(Equal(1))
						Ω(taskProcessor.ProcessCallCount()).Should(Equal(0))
						actualLogger, actualContainer := lrpProcessor.ProcessArgsForCall(0)
						Ω(actualLogger.SessionName()).Should(Equal(sessionName))
						Ω(actualContainer).Should(Equal(container))
					})
				})

				Context("when the container has a Task lifecycle tag", func() {
					BeforeEach(func() {
						container = executor.Container{
							Tags: executor.Tags{
								rep.LifecycleTag: rep.TaskLifecycle,
							},
						}
						containerDelegate.GetContainerReturns(container, true)
					})

					It("farms the container out to only the task processor", func() {
						Ω(taskProcessor.ProcessCallCount()).Should(Equal(1))
						Ω(lrpProcessor.ProcessCallCount()).Should(Equal(0))
						actualLogger, actualContainer := taskProcessor.ProcessArgsForCall(0)
						Ω(actualLogger.SessionName()).Should(Equal(sessionName))
						Ω(actualContainer).Should(Equal(container))
					})
				})

				Context("when the container has an unknown lifecycle tag", func() {
					BeforeEach(func() {
						container = executor.Container{
							Tags: executor.Tags{
								rep.LifecycleTag: "some-other-tag",
							},
						}
						containerDelegate.GetContainerReturns(container, true)
					})

					It("does not farm the container out to any processor", func() {
						Ω(lrpProcessor.ProcessCallCount()).Should(Equal(0))
						Ω(taskProcessor.ProcessCallCount()).Should(Equal(0))
					})

					It("logs the unknown lifecycle", func() {
						Ω(logger).Should(Say(sessionName + ".failed-to-process-container-with-unknown-lifecycle"))
					})
				})
			})
		})
	})
})
