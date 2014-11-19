package harvester_test

import (
	"sync"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/harvester"
	"github.com/cloudfoundry-incubator/rep/harvester/fakes"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Container Processor", func() {
	var (
		lrpTags  = executor.Tags{rep.LifecycleTag: rep.LRPLifecycle}
		taskTags = executor.Tags{rep.LifecycleTag: rep.TaskLifecycle}

		taskProcessor *fakes.FakeProcessor
		lrpProcessor  *fakes.FakeProcessor

		taskContainer executor.Container
		lrpContainer  executor.Container

		container          executor.Container
		containerProcessor harvester.Processor
	)

	BeforeEach(func() {
		taskProcessor = new(fakes.FakeProcessor)
		lrpProcessor = new(fakes.FakeProcessor)

		taskContainer = executor.Container{
			Guid:  "task-container-guid",
			State: executor.StateCompleted,
			Tags:  taskTags,
		}

		lrpContainer = executor.Container{
			Guid:  "lrp-container-guid",
			State: executor.StateCreated,
			Tags:  lrpTags,
		}

		containerProcessor = harvester.NewContainerProcessor(
			lagertest.NewTestLogger("test"),
			taskProcessor,
			lrpProcessor,
		)
	})

	Context("when processing a single container", func() {
		JustBeforeEach(func() {
			containerProcessor.Process(container)
		})

		Context("when the container is tagged with a task lifecycle", func() {
			BeforeEach(func() {
				container = taskContainer
			})

			It("should delegate processing to the task processor", func() {
				Eventually(taskProcessor.ProcessCallCount).Should(Equal(1))
				Consistently(lrpProcessor.ProcessCallCount).Should(Equal(0))
			})
		})

		Context("when the container is tagged with an lrp lifecycle", func() {
			BeforeEach(func() {
				container = lrpContainer
			})

			It("should delegate processing to the lrp processor", func() {
				Eventually(lrpProcessor.ProcessCallCount).Should(Equal(1))
				Consistently(taskProcessor.ProcessCallCount).Should(Equal(0))
			})
		})

		Context("when the container is missing a lifecyle tag", func() {
			BeforeEach(func() {
				container = lrpContainer
				container.Tags = executor.Tags{}
			})

			It("should skip processing the container", func() {
				Consistently(taskProcessor.ProcessCallCount).Should(Equal(0))
				Consistently(lrpProcessor.ProcessCallCount).Should(Equal(0))
			})
		})

		Context("when the container has no tags", func() {
			BeforeEach(func() {
				container = lrpContainer
				container.Tags = nil
			})

			It("should skip processing the container", func() {
				Consistently(taskProcessor.ProcessCallCount).Should(Equal(0))
				Consistently(lrpProcessor.ProcessCallCount).Should(Equal(0))
			})
		})
	})

	Context("when multiple process requests arrive at once", func() {
		Context("when multiple containers need processing", func() {
			var waitGroup *sync.WaitGroup

			BeforeEach(func() {
				waitGroup = &sync.WaitGroup{}
				waitGroup.Add(2)

				lrpProcessor.ProcessStub = func(c executor.Container) {
					waitGroup.Done()
					waitGroup.Wait()
				}

				taskProcessor.ProcessStub = func(c executor.Container) {
					waitGroup.Done()
					waitGroup.Wait()
				}
			})

			It("processes containers in parallel", func() {
				done := make(chan struct{})
				go func() {
					containerProcessor.Process(lrpContainer)
					close(done)
				}()
				Eventually(done).Should(BeClosed())

				containerProcessor.Process(taskContainer)

				waitGroup.Wait()

				Ω(lrpProcessor.ProcessArgsForCall(0)).Should(Equal(lrpContainer))
				Ω(taskProcessor.ProcessArgsForCall(0)).Should(Equal(taskContainer))
			})
		})

		Context("when multiple requests arrive for a single container", func() {
			var (
				container1 executor.Container
				container2 executor.Container
				container3 executor.Container
				waitChan   chan struct{}
			)

			BeforeEach(func() {
				container1 = executor.Container{
					Guid:   "lrp-container-guid",
					State:  executor.StateCreated,
					Health: executor.HealthDown,
					Tags:   lrpTags,
				}

				container2 = executor.Container{
					Guid:   "lrp-container-guid",
					State:  executor.StateCreated,
					Health: executor.HealthUp,
					Tags:   lrpTags,
				}

				container3 = executor.Container{
					Guid:   "lrp-container-guid",
					State:  executor.StateCompleted,
					Health: executor.HealthDown,
					Tags:   lrpTags,
					RunResult: executor.ContainerRunResult{
						Failed: false,
					},
				}

				waitChan = make(chan struct{})
				lrpProcessor.ProcessStub = func(c executor.Container) {
					<-waitChan
				}
			})

			It("discards stale requests", func() {
				containerProcessor.Process(container1)
				Eventually(lrpProcessor.ProcessCallCount).Should(Equal(1))
				Consistently(lrpProcessor.ProcessCallCount).Should(Equal(1))

				Eventually(waitChan).Should(BeSent(struct{}{}))

				containerProcessor.Process(container2)
				containerProcessor.Process(container3)
				Eventually(waitChan).Should(BeSent(struct{}{}))

				Eventually(lrpProcessor.ProcessCallCount).Should(Equal(2))
				Consistently(lrpProcessor.ProcessCallCount).Should(Equal(2))

				Ω(lrpProcessor.ProcessArgsForCall(0)).Should(Equal(container1))
				Ω(lrpProcessor.ProcessArgsForCall(1)).Should(Equal(container3))

				By("work processing complete", func() {
					Ω(containerProcessor.(harvester.ProcessorQueue).WorkPending()).Should(BeFalse())
				})
			})
		})
	})
})
