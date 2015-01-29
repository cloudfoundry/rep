package evacuation_test

import (
	"errors"
	"strconv"
	"syscall"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/evacuation"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context"
	"github.com/cloudfoundry-incubator/rep/generator"
	"github.com/cloudfoundry-incubator/rep/generator/internal/fake_internal"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/pivotal-golang/clock/fakeclock"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/pivotal-golang/operationq/fake_operationq"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Evacuation", func() {
	var (
		process            ifrit.Process
		evacuator          *evacuation.Evacuator
		logger             *lagertest.TestLogger
		evacuatable        evacuation_context.Evacuatable
		evacuationReporter evacuation_context.EvacuationReporter
		evacuationTimeout  time.Duration
		containerDelegate  *fake_internal.FakeContainerDelegate
		lrpProcessor       *fake_internal.FakeLRPProcessor
		taskProcessor      *fake_internal.FakeTaskProcessor
		fakeClock          *fakeclock.FakeClock
		executorClient     *fakes.FakeClient
		bbs                *fake_bbs.FakeRepBBS
		fakeQueue          *fake_operationq.FakeQueue

		cellID   string
		TaskTags map[string]string
		LRPTags  func(string, int) map[string]string
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test")
		executorClient = &fakes.FakeClient{}
		bbs = &fake_bbs.FakeRepBBS{}
		lrpProcessor = new(fake_internal.FakeLRPProcessor)
		taskProcessor = new(fake_internal.FakeTaskProcessor)
		containerDelegate = new(fake_internal.FakeContainerDelegate)
		fakeQueue = new(fake_operationq.FakeQueue)
		evacuationTimeout = 3 * time.Minute
		pollingInterval := 30 * time.Second
		fakeClock = fakeclock.NewFakeClock(time.Now())

		cellID = "cell-id"

		evacuatable, evacuationReporter = evacuation_context.New()
		evacuator = evacuation.NewEvacuator(
			logger,
			executorClient,
			bbs,
			evacuatable,
			lrpProcessor,
			taskProcessor,
			containerDelegate,
			fakeQueue,
			cellID,
			evacuationTimeout,
			pollingInterval,
			fakeClock,
		)

		process = ifrit.Invoke(evacuator)

		TaskTags = map[string]string{rep.LifecycleTag: rep.TaskLifecycle}
		LRPTags = func(processGuid string, index int) map[string]string {
			return map[string]string{
				rep.LifecycleTag:    rep.LRPLifecycle,
				rep.DomainTag:       "domain",
				rep.ProcessGuidTag:  processGuid,
				rep.ProcessIndexTag: strconv.Itoa(index),
			}
		}
	})

	Describe("Signal", func() {
		Context("SIGUSR1", func() {
			It("causes the evacuationReporter to report Evacuation is underway", func() {
				process.Signal(syscall.SIGUSR1)
				Eventually(evacuationReporter.Evacuating).Should(BeTrue())
			})

			It("exits after the evacuationTimeout has elapsed", func() {
				exitedCh := make(chan struct{})
				go func() {
					<-process.Wait()
					close(exitedCh)
				}()

				containers := []executor.Container{
					{Guid: "guid-1", State: executor.StateRunning, Tags: TaskTags},
				}
				executorClient.ListContainersReturns(containers, nil)
				process.Signal(syscall.SIGUSR1)
				Eventually(fakeClock.WatcherCount).Should(Equal(2))

				fakeClock.IncrementBySeconds(179)
				Consistently(exitedCh).ShouldNot(BeClosed())
				fakeClock.IncrementBySeconds(2)
				Eventually(exitedCh).Should(BeClosed())
			})

			Context("when there are tasks to evacuate", func() {
				var (
					containers [][]executor.Container
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
			})

			Context("when there are actualLRPs", func() {
				var (
					containers [][]executor.Container
				)

				BeforeEach(func() {
					containers = [][]executor.Container{
						{
							{Guid: "task-guid-1", State: executor.StateCompleted, Tags: TaskTags},
							{Guid: "lrp-guid-1", State: executor.StateReserved, Tags: LRPTags("process-guid-1", 1)},
							{Guid: "lrp-guid-2", State: executor.StateCreated, Tags: LRPTags("process-guid-2", 2)},
							{Guid: "lrp-guid-3", State: executor.StateRunning, Tags: LRPTags("process-guid-3", 3)},
						},
						{
							{Guid: "task-guid-1", State: executor.StateCompleted, Tags: TaskTags},
							{Guid: "lrp-guid-2", State: executor.StateRunning, Tags: LRPTags("process-guid-2", 2)},
							{Guid: "lrp-guid-3", State: executor.StateRunning, Tags: LRPTags("process-guid-3", 3)},
						},
					}

					index := 0
					executorClient.ListContainersStub = func(executor.Tags) ([]executor.Container, error) {
						containersToReturn := containers[index]
						index++
						return containersToReturn, nil
					}

					process.Signal(syscall.SIGUSR1)
				})

				It("puts an operation for each lrp container onto the operation queue", func() {
					Eventually(fakeQueue.PushCallCount).Should(Equal(3))
					fakeClock.IncrementBySeconds(30)
					Eventually(fakeQueue.PushCallCount).Should(Equal(5))

					keys := []string{}
					for i := 0; i < 5; i++ {
						pushArg := fakeQueue.PushArgsForCall(i)

						containerOperation := generator.ContainerOperation{}
						Ω(pushArg).Should(BeAssignableToTypeOf(&containerOperation))

						keys = append(keys, pushArg.Key())
					}

					Ω(keys).Should(ConsistOf("lrp-guid-1", "lrp-guid-2", "lrp-guid-3", "lrp-guid-2", "lrp-guid-3"))
				})
			})
		})

		Context("any other signal", func() {
			BeforeEach(func() {
				process.Signal(syscall.SIGINT)
			})

			It("does not cause the evacuationReporter to report Evacuation is underway", func() {
				Consistently(evacuationReporter.Evacuating).Should(BeFalse())
			})

			It("does not wait for evacuation before exiting", func() {
				wait := process.Wait()
				Eventually(wait).Should(Receive())
				Consistently(fakeClock.WatcherCount).Should(Equal(0))
			})
		})
	})
})
