package evacuation_test

import (
	"errors"
	"syscall"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/evacuation"
	"github.com/pivotal-golang/clock/fakeclock"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Evacuation", func() {
	var (
		process           ifrit.Process
		evacuator         *evacuation.Evacuator
		logger            *lagertest.TestLogger
		evacuationContext evacuation.EvacuationContext
		evacuationTimeout time.Duration
		fakeClock         *fakeclock.FakeClock
		executorClient    *fakes.FakeClient
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test")
		fakeClock = fakeclock.NewFakeClock(time.Now())
		pollingInterval := 30 * time.Second
		executorClient = &fakes.FakeClient{}
		evacuationTimeout = 3 * time.Minute
		evacuator = evacuation.NewEvacuator(logger, executorClient, evacuationTimeout, pollingInterval, fakeClock)
		evacuationContext = evacuator.EvacuationContext()
		process = ifrit.Invoke(evacuator)
	})

	Describe("Signal", func() {
		Context("SIGUSR1", func() {
			var (
				containers [][]executor.Container
				TaskTags   = map[string]string{rep.LifecycleTag: rep.TaskLifecycle}
			)

			BeforeEach(func() {
				containers = [][]executor.Container{
					{
						{Guid: "guid-1", State: executor.StateRunning, Tags: TaskTags},
						{Guid: "guid-2", State: executor.StateCreated, Tags: TaskTags},
						{Guid: "guid-3", State: executor.StateCompleted, Tags: TaskTags},
					},
					{
						{Guid: "guid-1", State: executor.StateRunning, Tags: TaskTags},
						{Guid: "guid-2", State: executor.StateRunning, Tags: TaskTags},
					},
					{
						{Guid: "guid-1", State: executor.StateCompleted, Tags: TaskTags},
						{Guid: "guid-2", State: executor.StateCompleted, Tags: TaskTags},
					},
				}

				index := 0
				executorClient.ListContainersStub = func(executor.Tags) ([]executor.Container, error) {
					containersToReturn := containers[index]
					index++
					return containersToReturn, nil
				}
			})

			It("flips the bit on the evacuationContext", func() {
				process.Signal(syscall.SIGUSR1)
				Eventually(evacuationContext.Evacuating).Should(BeTrue())
			})

			It("waits for all tasks to complete before exiting", func() {
				exitedCh := make(chan struct{})
				go func() {
					<-process.Wait()
					close(exitedCh)
				}()

				process.Signal(syscall.SIGUSR1)

				Eventually(executorClient.ListContainersCallCount).Should(Equal(1))
				fakeClock.IncrementBySeconds(30)
				Eventually(executorClient.ListContainersCallCount).Should(Equal(2))
				fakeClock.IncrementBySeconds(30)
				Eventually(executorClient.ListContainersCallCount).Should(Equal(3))
				Eventually(exitedCh).Should(BeClosed())
			})

			Context("when getting the containers results in an error", func() {
				BeforeEach(func() {
					callCount := 0
					executorClient.ListContainersStub = func(executor.Tags) ([]executor.Container, error) {
						if callCount == 0 {
							callCount++
							return []executor.Container{}, errors.New("error")
						}
						return containers[2], nil
					}
				})

				It("retries", func() {
					exitedCh := make(chan struct{})
					go func() {
						<-process.Wait()
						close(exitedCh)
					}()

					process.Signal(syscall.SIGUSR1)
					Eventually(executorClient.ListContainersCallCount).Should(Equal(1))
					fakeClock.IncrementBySeconds(30)
					Eventually(executorClient.ListContainersCallCount).Should(Equal(2))
					Eventually(exitedCh).Should(BeClosed())
				})
			})

			It("exits after the evacuationTimeout has elapsed", func() {
				exitedCh := make(chan struct{})
				go func() {
					<-process.Wait()
					close(exitedCh)
				}()

				process.Signal(syscall.SIGUSR1)
				Eventually(fakeClock.WatcherCount).Should(Equal(2))

				fakeClock.IncrementBySeconds(179)
				Consistently(exitedCh).ShouldNot(BeClosed())
				fakeClock.IncrementBySeconds(2)
				Eventually(exitedCh).Should(BeClosed())
			})
		})

		Context("any other signal", func() {
			BeforeEach(func() {
				process.Signal(syscall.SIGINT)
			})

			It("does not flip the bit on the evacuationContext", func() {
				Consistently(evacuationContext.Evacuating).Should(BeFalse())
			})

			It("does not wait for evacuation before exiting", func() {
				wait := process.Wait()
				Eventually(wait).Should(Receive())
				Consistently(fakeClock.WatcherCount).Should(Equal(0))
			})
		})
	})
})
