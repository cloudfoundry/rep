package evacuation_test

import (
	"errors"
	"os"
	"time"

	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/evacuation"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context"
	fake_metrics_sender "github.com/cloudfoundry/dropsonde/metric_sender/fake"
	"github.com/cloudfoundry/dropsonde/metrics"
	"github.com/pivotal-golang/clock/fakeclock"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Evacuation", func() {
	const (
		cellID            = "cell-id"
		pollingInterval   = 30 * time.Second
		evacuationTimeout = time.Duration(6) * pollingInterval
	)

	var (
		logger             *lagertest.TestLogger
		fakeClock          *fakeclock.FakeClock
		executorClient     *fakes.FakeClient
		evacuatable        evacuation_context.Evacuatable
		evacuationNotifier evacuation_context.EvacuationNotifier

		evacuator *evacuation.Evacuator
		process   ifrit.Process

		errChan chan error

		TaskTags         map[string]string
		LRPTags          map[string]string
		containers       []executor.Container
		fakeMetricSender *fake_metrics_sender.FakeMetricSender
	)

	BeforeEach(func() {
		fakeMetricSender = fake_metrics_sender.NewFakeMetricSender()
		metrics.Initialize(fakeMetricSender, nil)
		logger = lagertest.NewTestLogger("test")
		fakeClock = fakeclock.NewFakeClock(time.Now())
		executorClient = &fakes.FakeClient{}

		evacuatable, _, evacuationNotifier = evacuation_context.New()

		evacuator = evacuation.NewEvacuator(
			fakeBBSClient,
			logger,
			fakeClock,
			executorClient,
			evacuationNotifier,
			cellID,
			evacuationTimeout,
			pollingInterval,
		)

		process = ifrit.Invoke(evacuator)

		errChan = make(chan error, 1)

		localErrChan := errChan
		evacuationProcess := process
		go func() {
			localErrChan <- <-evacuationProcess.Wait()
		}()

		TaskTags = map[string]string{rep.LifecycleTag: rep.TaskLifecycle}
		LRPTags = map[string]string{
			rep.LifecycleTag:    rep.LRPLifecycle,
			rep.DomainTag:       "domain",
			rep.ProcessGuidTag:  "process-guid",
			rep.ProcessIndexTag: "2",
		}
		containers = []executor.Container{
			{Guid: "guid-1", State: executor.StateRunning, Tags: TaskTags},
			{Guid: "guid-2", State: executor.StateRunning, Tags: LRPTags},
		}
	})

	Describe("before evacuating", func() {
		It("exits when interrupted", func() {
			process.Signal(os.Interrupt)

			Eventually(errChan).Should(Receive(BeNil()))
		})
	})

	Describe("during evacuation", func() {
		JustBeforeEach(func() {
			evacuatable.Evacuate()
		})

		Context("when containers are present", func() {
			var expectedActualLRPKey *models.ActualLRPKey
			var expectedActualLRPInstanceKey *models.ActualLRPInstanceKey

			BeforeEach(func() {
				expectedActualLRPKey = &models.ActualLRPKey{ProcessGuid: "process-guid"}
				expectedActualLRPInstanceKey = &models.ActualLRPInstanceKey{InstanceGuid: "guid-2", CellId: cellID}

				evacuatingOnlyLRP := models.ActualLRP{ActualLRPKey: *expectedActualLRPKey, ActualLRPInstanceKey: *expectedActualLRPInstanceKey}

				lrpGroups := []*models.ActualLRPGroup{
					{Instance: nil, Evacuating: &evacuatingOnlyLRP},
				}

				fakeBBSClient.ActualLRPGroupsReturns(lrpGroups, nil)
			})

			Context("and are all destroyed before the timeout elapses", func() {
				BeforeEach(func() {
					containerResponses := [][]executor.Container{
						containers,
						[]executor.Container{},
					}

					index := 0
					executorClient.ListContainersStub = func(lager.Logger) ([]executor.Container, error) {
						containersToReturn := containerResponses[index]
						index++
						return containersToReturn, nil
					}

				})

				Context("the client does not error", func() {

					JustBeforeEach(func() {
						fakeClock.Increment(pollingInterval)
						Eventually(executorClient.ListContainersCallCount).Should(Equal(1))

						fakeClock.Increment(pollingInterval)
						Eventually(executorClient.ListContainersCallCount).Should(Equal(2))
					})

					It("waits for all the containers to go away and exits before evacuation timeout", func() {
						Eventually(errChan).Should(Receive(BeNil()))
					})

					It("Deletes any remaining evacuating ActualLRPs it has claimed", func() {
						Eventually(fakeBBSClient.RemoveEvacuatingActualLRPCallCount()).Should(Equal(1))
						actualLRPKey, actualLRPInstanceKey := fakeBBSClient.RemoveEvacuatingActualLRPArgsForCall(0)
						Expect(actualLRPKey).To(Equal(expectedActualLRPKey))
						Expect(actualLRPInstanceKey).To(Equal(expectedActualLRPInstanceKey))
					})

					It("emits a metric on the number of ActualLRPs removed", func() {
						Eventually(func() float64 {
							return fakeMetricSender.GetValue("StrandedEvacuatedActualLRPs").Value
						}).Should(Equal(float64(1)))
					})
				})

				Context("when the executor client returns an error", func() {
					BeforeEach(func() {
						index := 0
						executorClient.ListContainersStub = func(lager.Logger) ([]executor.Container, error) {
							if index == 0 {
								index++
								return nil, errors.New("whoops")
							}
							return []executor.Container{}, nil
						}
					})

					It("retries", func() {
						fakeClock.Increment(pollingInterval)
						Eventually(executorClient.ListContainersCallCount).Should(Equal(1))

						fakeClock.Increment(pollingInterval)
						Eventually(executorClient.ListContainersCallCount).Should(Equal(2))

						Eventually(errChan).Should(Receive(BeNil()))
					})
				})
			})

			Context("and are not all destroyed before the timeout elapses", func() {
				BeforeEach(func() {
					executorClient.ListContainersReturns(containers, nil)
				})

				JustBeforeEach(func() {
					Eventually(fakeClock.WatcherCount).Should(Equal(2))
					fakeClock.Increment(evacuationTimeout - time.Second)
					Consistently(errChan).ShouldNot(Receive())
					fakeClock.Increment(2 * time.Second)
				})

				It("exits after the evacuation timeout", func() {
					Eventually(errChan).Should(Receive(BeNil()))
				})

				It("Deletes any remaining evacuating ActualLRPs it has claimed", func() {
					Eventually(errChan).Should(Receive(BeNil()))

					Eventually(fakeBBSClient.RemoveEvacuatingActualLRPCallCount()).Should(Equal(1))
					actualLRPKey, actualLRPInstanceKey := fakeBBSClient.RemoveEvacuatingActualLRPArgsForCall(0)
					Expect(actualLRPKey).To(Equal(expectedActualLRPKey))
					Expect(actualLRPInstanceKey).To(Equal(expectedActualLRPInstanceKey))
				})

				It("emits a metric on the number of ActualLRPs removed", func() {
					Eventually(errChan).Should(Receive(BeNil()))

					Expect(fakeMetricSender.GetValue("StrandedEvacuatedActualLRPs").Value).To(Equal(float64(1)))
				})

				Context("when signaled", func() {
					It("exits", func() {
						process.Signal(os.Interrupt)

						Eventually(errChan).Should(Receive(BeNil()))
					})
				})
			})
		})
	})
})
