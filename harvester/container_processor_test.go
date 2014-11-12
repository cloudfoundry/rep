package harvester_test

import (
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

	JustBeforeEach(func() {
		containerProcessor.Process(container)
	})

	Context("when the container is tagged with a task lifecycle", func() {
		BeforeEach(func() {
			container = taskContainer
		})

		It("should delegate processing to the task processor", func() {
			Ω(taskProcessor.ProcessCallCount()).Should(Equal(1))
			Ω(lrpProcessor.ProcessCallCount()).Should(Equal(0))
		})
	})

	Context("when the container is tagged with an lrp lifecycle", func() {
		BeforeEach(func() {
			container = lrpContainer
		})

		It("should delegate processing to the lrp processor", func() {
			Ω(lrpProcessor.ProcessCallCount()).Should(Equal(1))
			Ω(taskProcessor.ProcessCallCount()).Should(Equal(0))
		})
	})

	Context("when the container is missing a lifecyle tag", func() {
		BeforeEach(func() {
			container = lrpContainer
			container.Tags = executor.Tags{}
		})

		It("should skip processing the container", func() {
			Ω(taskProcessor.ProcessCallCount()).Should(Equal(0))
			Ω(lrpProcessor.ProcessCallCount()).Should(Equal(0))
		})
	})

	Context("when the container has no tags", func() {
		BeforeEach(func() {
			container = lrpContainer
			container.Tags = nil
		})

		It("should skip processing the container", func() {
			Ω(taskProcessor.ProcessCallCount()).Should(Equal(0))
			Ω(lrpProcessor.ProcessCallCount()).Should(Equal(0))
		})
	})
})
