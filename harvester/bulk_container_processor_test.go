package harvester_test

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/gatherer/fake_gatherer"
	"github.com/cloudfoundry-incubator/rep/harvester"
	"github.com/cloudfoundry-incubator/rep/harvester/fakes"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bulk Container Processor", func() {
	var (
		bulkContainerProcessor *harvester.BulkContainerProcessor
		processor              *fakes.FakeProcessor
		snapshot               *fake_gatherer.FakeSnapshot
	)

	BeforeEach(func() {
		processor = new(fakes.FakeProcessor)
		snapshot = new(fake_gatherer.FakeSnapshot)
	})

	JustBeforeEach(func() {
		bulkContainerProcessor = harvester.NewBulkContainerProcessor(processor, lagertest.NewTestLogger("test"))
		bulkContainerProcessor.Process(snapshot)
	})

	var lrpTags = executor.Tags{rep.LifecycleTag: rep.LRPLifecycle}
	var taskTags = executor.Tags{rep.LifecycleTag: rep.TaskLifecycle}

	It("polls executor for containers", func() {
		Ω(snapshot.ListContainersCallCount()).Should(Equal(1))
		Ω(snapshot.ListContainersArgsForCall(0)).Should(BeNil())
	})

	Context("and the executor returns tasks and lrps", func() {
		containers := []executor.Container{
			{Guid: "first-completed-task-guid", State: executor.StateCompleted, Tags: taskTags},
			{Guid: "second-completed-task-guid", State: executor.StateCompleted, Tags: taskTags},
			{Guid: "first-completed-lrp-guid", State: executor.StateCompleted, Tags: lrpTags},
			{Guid: "first-completed-foobar-guid", State: executor.StateCompleted, Tags: executor.Tags{rep.LifecycleTag: "foobar"}},
			{Guid: "created-task-guid", State: executor.StateCreated, Tags: taskTags},
			{Guid: "initializing-task-guid", State: executor.StateInitializing, Tags: taskTags},
			{Guid: "reserved-task-guid", State: executor.StateReserved, Tags: taskTags},
		}

		BeforeEach(func() {
			snapshot.ListContainersReturns(containers)
		})

		It("processes all returned containers", func() {
			Ω(processor.ProcessCallCount()).Should(Equal(7))

			for i := 0; i < 7; i++ {
				Ω(processor.ProcessArgsForCall(i)).Should(Equal(containers[i]))
			}
		})
	})
})
