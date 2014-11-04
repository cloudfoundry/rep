package tallyman_test

import (
	"errors"
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	efakes "github.com/cloudfoundry-incubator/executor/fakes"
	. "github.com/cloudfoundry-incubator/rep/tallyman"
	"github.com/cloudfoundry-incubator/rep/tallyman/fakes"
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

		poller = NewPoller(pollInterval, timer, executorClient, processor)
	})

	JustBeforeEach(func() {
		process = ifrit.Invoke(poller)
	})

	AfterEach(func() {
		process.Signal(os.Interrupt)
		Eventually(process.Wait()).Should(Receive())
	})

	Context("when the timer elapses", func() {
		JustBeforeEach(func() {
			timer.Elapse(pollInterval)
		})

		It("polls executor for tasks", func() {
			Eventually(executorClient.ListContainersCallCount).Should(Equal(1))

			Ω(executorClient.ListContainersArgsForCall(0)).Should(Equal(executor.Tags{
				LifecycleTag: TaskLifecycle,
			}))
		})

		Context("and the executor returns completed containers", func() {
			BeforeEach(func() {
				executorClient.ListContainersReturns([]executor.Container{
					{Guid: "first-completed-guid", State: executor.StateCompleted},
					{Guid: "second-completed-guid", State: executor.StateCompleted},
					{Guid: "created-guid", State: executor.StateCreated},
					{Guid: "initializing-guid", State: executor.StateInitializing},
					{Guid: "reserved-guid", State: executor.StateReserved},
				}, nil)
			})

			It("processes each of them", func() {
				Eventually(processor.ProcessCallCount).Should(Equal(2))

				Ω(processor.ProcessArgsForCall(0)).Should(Equal(executor.Container{
					Guid:  "first-completed-guid",
					State: executor.StateCompleted,
				}))

				Ω(processor.ProcessArgsForCall(1)).Should(Equal(executor.Container{
					Guid:  "second-completed-guid",
					State: executor.StateCompleted,
				}))
			})

			It("does not process the non-completed containers", func() {
				Eventually(processor.ProcessCallCount).Should(Equal(2))
				Consistently(processor.ProcessCallCount).Should(Equal(2))
			})
		})

		Context("when the executor returns no completed containers", func() {
			BeforeEach(func() {
				executorClient.ListContainersReturns([]executor.Container{
					{Guid: "created-guid", State: executor.StateCreated},
					{Guid: "initializing-guid", State: executor.StateInitializing},
					{Guid: "reserved-guid", State: executor.StateReserved},
				}, nil)
			})

			It("doesn't process anything", func() {
				Consistently(processor.ProcessCallCount()).Should(BeZero())
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
