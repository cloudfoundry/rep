package harvester_test

import (
	"errors"
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	efakes "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	. "github.com/cloudfoundry-incubator/rep/harvester"
	"github.com/cloudfoundry-incubator/rep/harvester/fakes"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/pivotal-golang/timer/fake_timer"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Poller", func() {
	var (
		executorClient *efakes.FakeClient
		processor      *fakes.FakeProcessor

		pollInterval time.Duration
		timer        *fake_timer.FakeTimer
		poller       ifrit.Runner
		process      ifrit.Process
	)

	BeforeEach(func() {
		pollInterval = 100 * time.Millisecond
		timer = fake_timer.NewFakeTimer(time.Now())
		executorClient = new(efakes.FakeClient)
		processor = new(fakes.FakeProcessor)

		poller = NewPoller(pollInterval, timer, executorClient, processor, lagertest.NewTestLogger("test"))
	})

	JustBeforeEach(func() {
		process = ifrit.Invoke(poller)
	})

	AfterEach(func() {
		process.Signal(os.Interrupt)
		Eventually(process.Wait()).Should(Receive())
	})

	Context("when the timer elapses", func() {
		var lrpTags = executor.Tags{rep.LifecycleTag: rep.LRPLifecycle}
		var taskTags = executor.Tags{rep.LifecycleTag: rep.TaskLifecycle}

		JustBeforeEach(func() {
			timer.Elapse(pollInterval)
		})

		It("polls executor for containers", func() {
			Eventually(executorClient.ListContainersCallCount).Should(Equal(1))

			Ω(executorClient.ListContainersArgsForCall(0)).Should(BeNil())
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
				executorClient.ListContainersReturns(containers, nil)
			})

			It("processes all returned containers", func() {
				Eventually(processor.ProcessCallCount).Should(Equal(7))

				for i := 0; i < 7; i++ {
					Ω(processor.ProcessArgsForCall(i)).Should(Equal(containers[i]))
				}
			})
		})

		Context("when listing containers fails", func() {
			disaster := errors.New("oh no!")

			BeforeEach(func() {
				executorClient.ListContainersReturns(nil, disaster)
			})

			It("does not die", func() {
				Consistently(process.Wait()).ShouldNot(Receive())
			})

			Context("and the timer elapses again", func() {
				JustBeforeEach(func() {
					timer.Elapse(pollInterval)
				})

				It("happily continues on to next time", func() {
					Eventually(executorClient.ListContainersCallCount).Should(Equal(2))
				})
			})
		})
	})
})
