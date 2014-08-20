package task_bbs_test

import (
	"os"
	"path"
	"time"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/services_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	. "github.com/cloudfoundry-incubator/runtime-schema/bbs/task_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gunk/timeprovider/faketimeprovider"
	"github.com/cloudfoundry/storeadapter"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"
)

var _ = Describe("Convergence of Tasks", func() {
	var bbs *TaskBBS
	var task models.Task
	var timeToClaimInSeconds, convergenceIntervalInSeconds uint64
	var timeToClaim, convergenceInterval time.Duration
	var timeProvider *faketimeprovider.FakeTimeProvider
	var err error
	var servicesBBS *services_bbs.ServicesBBS

	BeforeEach(func() {
		err = nil

		timeToClaimInSeconds = 30
		timeToClaim = time.Duration(timeToClaimInSeconds) * time.Second
		convergenceIntervalInSeconds = 10
		convergenceInterval = time.Duration(convergenceIntervalInSeconds) * time.Second

		timeProvider = faketimeprovider.New(time.Unix(1238, 0))

		task = models.Task{
			Domain:  "tests",
			Guid:    "some-guid",
			Stack:   "pancakes",
			Actions: dummyActions,
		}

		logger := lagertest.NewTestLogger("test")

		bbs = New(etcdClient, timeProvider, logger)

		servicesBBS = services_bbs.New(etcdClient, logger)
	})

	Describe("ConvergeTask", func() {
		var desiredEvents <-chan models.Task
		var completedEvents <-chan models.Task

		commenceWatching := func() {
			desiredEvents, _, _ = bbs.WatchForDesiredTask()
			completedEvents, _, _ = bbs.WatchForCompletedTask()
		}

		Context("when a Task is malformed", func() {
			It("should delete it", func() {
				nodeKey := path.Join(shared.TaskSchemaRoot, "some-guid")

				err := etcdClient.Create(storeadapter.StoreNode{
					Key:   nodeKey,
					Value: []byte("ß"),
				})
				Ω(err).ShouldNot(HaveOccurred())

				_, err = etcdClient.Get(nodeKey)
				Ω(err).ShouldNot(HaveOccurred())

				bbs.ConvergeTask(timeToClaim, convergenceInterval)

				_, err = etcdClient.Get(nodeKey)
				Ω(err).Should(Equal(storeadapter.ErrorKeyNotFound))
			})
		})

		Context("when a Task is pending", func() {
			BeforeEach(func() {
				err = bbs.DesireTask(task)
				Ω(err).ShouldNot(HaveOccurred())
			})

			Context("when the Task has *not* been pending for too long", func() {
				It("should not kick the Task", func() {
					timeProvider.IncrementBySeconds(1)
					commenceWatching()
					bbs.ConvergeTask(timeToClaim, convergenceInterval)

					Consistently(desiredEvents).ShouldNot(Receive())
				})
			})

			Context("when the Task has been pending for longer than the convergence interval", func() {
				It("should kick the Task", func() {
					timeProvider.IncrementBySeconds(convergenceIntervalInSeconds + 1)
					commenceWatching()
					bbs.ConvergeTask(timeToClaim, convergenceInterval)

					var noticedOnce models.Task
					Eventually(desiredEvents).Should(Receive(&noticedOnce))

					Ω(noticedOnce.Guid).Should(Equal(task.Guid))
					Ω(noticedOnce.State).Should(Equal(models.TaskStatePending))
					Ω(noticedOnce.UpdatedAt).Should(Equal(timeProvider.Time().UnixNano()))
				})
			})

			Context("when the Task has been pending for longer than the timeToClaim", func() {
				It("should mark the Task as completed & failed", func() {
					timeProvider.IncrementBySeconds(timeToClaimInSeconds + 1)
					commenceWatching()
					bbs.ConvergeTask(timeToClaim, convergenceInterval)

					Consistently(desiredEvents).ShouldNot(Receive())

					var noticedOnce models.Task
					Eventually(completedEvents).Should(Receive(&noticedOnce))

					Ω(noticedOnce.Failed).Should(Equal(true))
					Ω(noticedOnce.FailureReason).Should(ContainSubstring("time limit"))
				})
			})
		})

		Context("when a Task is claimed", func() {
			var heartbeat ifrit.Process

			BeforeEach(func() {
				err = bbs.DesireTask(task)
				Ω(err).ShouldNot(HaveOccurred())

				err = bbs.ClaimTask(task.Guid, "executor-id")
				Ω(err).ShouldNot(HaveOccurred())

				executorPresence := models.ExecutorPresence{
					ExecutorID: "executor-id",
				}
				heartbeat = ifrit.Envoke(servicesBBS.NewExecutorHeartbeat(executorPresence, time.Minute))
			})

			AfterEach(func() {
				heartbeat.Signal(os.Interrupt)
				Eventually(heartbeat.Wait()).Should(Receive(BeNil()))
			})

			It("should do nothing", func() {
				commenceWatching()

				bbs.ConvergeTask(timeToClaim, convergenceInterval)

				Consistently(desiredEvents).ShouldNot(Receive())
				Consistently(completedEvents).ShouldNot(Receive())
			})

			Context("when the associated executor is missing", func() {
				BeforeEach(func() {
					heartbeat.Signal(os.Interrupt)
					Eventually(heartbeat.Wait()).Should(Receive(BeNil()))
				})

				It("should mark the Task as completed & failed", func() {
					timeProvider.IncrementBySeconds(1)
					commenceWatching()

					bbs.ConvergeTask(timeToClaim, convergenceInterval)

					Consistently(desiredEvents).ShouldNot(Receive())

					var noticedOnce models.Task
					Eventually(completedEvents).Should(Receive(&noticedOnce))

					Ω(noticedOnce.Failed).Should(Equal(true))
					Ω(noticedOnce.FailureReason).Should(ContainSubstring("executor"))
					Ω(noticedOnce.UpdatedAt).Should(Equal(timeProvider.Time().UnixNano()))
				})
			})
		})

		Context("when a Task is running", func() {
			var heartbeater ifrit.Process

			BeforeEach(func() {
				err = bbs.DesireTask(task)
				Ω(err).ShouldNot(HaveOccurred())

				err = bbs.ClaimTask(task.Guid, "executor-id")
				Ω(err).ShouldNot(HaveOccurred())

				err = bbs.StartTask(task.Guid, "executor-id", "container-handle")
				Ω(err).ShouldNot(HaveOccurred())

				heartbeater = ifrit.Envoke(servicesBBS.NewExecutorHeartbeat(models.ExecutorPresence{
					ExecutorID: "executor-id",
				}, time.Minute))
			})

			AfterEach(func() {
				heartbeater.Signal(os.Interrupt)
			})

			It("should do nothing", func() {
				commenceWatching()

				bbs.ConvergeTask(timeToClaim, convergenceInterval)

				Consistently(desiredEvents).ShouldNot(Receive())
				Consistently(completedEvents).ShouldNot(Receive())
			})

			Context("when the associated executor is missing", func() {
				BeforeEach(func() {
					heartbeater.Signal(os.Interrupt)
				})

				It("should mark the Task as completed & failed", func() {
					timeProvider.IncrementBySeconds(1)
					commenceWatching()

					bbs.ConvergeTask(timeToClaim, convergenceInterval)

					Consistently(desiredEvents).ShouldNot(Receive())

					var noticedOnce models.Task
					Eventually(completedEvents).Should(Receive(&noticedOnce))

					Ω(noticedOnce.Failed).Should(Equal(true))
					Ω(noticedOnce.FailureReason).Should(ContainSubstring("executor"))
					Ω(noticedOnce.UpdatedAt).Should(Equal(timeProvider.Time().UnixNano()))
				})
			})
		})

		Context("when a Task is completed", func() {
			BeforeEach(func() {
				err = bbs.DesireTask(task)
				Ω(err).ShouldNot(HaveOccurred())

				err = bbs.ClaimTask(task.Guid, "executor-id")
				Ω(err).ShouldNot(HaveOccurred())

				err = bbs.StartTask(task.Guid, "executor-id", "container-handle")
				Ω(err).ShouldNot(HaveOccurred())

				err = bbs.CompleteTask(task.Guid, true, "'cause I said so", "a magical result")
				Ω(err).ShouldNot(HaveOccurred())
			})

			Context("when the task has been completed for > the convergence interval", func() {
				It("should kick the Task", func() {
					timeProvider.IncrementBySeconds(convergenceIntervalInSeconds + 1)
					commenceWatching()

					bbs.ConvergeTask(timeToClaim, convergenceInterval)

					Consistently(desiredEvents).ShouldNot(Receive())

					var noticedOnce models.Task
					Eventually(completedEvents).Should(Receive(&noticedOnce))

					Ω(noticedOnce.Failed).Should(Equal(true))
					Ω(noticedOnce.FailureReason).Should(Equal("'cause I said so"))
					Ω(noticedOnce.Result).Should(Equal("a magical result"))
					Ω(noticedOnce.UpdatedAt).Should(Equal(timeProvider.Time().UnixNano()))
				})
			})

			Context("when the task has been completed for < the convergence interval", func() {
				It("should kick the Task", func() {
					timeProvider.IncrementBySeconds(1)
					commenceWatching()

					bbs.ConvergeTask(timeToClaim, convergenceInterval)

					Consistently(desiredEvents).ShouldNot(Receive())
					Consistently(completedEvents).ShouldNot(Receive())
				})
			})
		})

		Context("when a Task is resolving", func() {
			BeforeEach(func() {
				err = bbs.DesireTask(task)
				Ω(err).ShouldNot(HaveOccurred())

				err = bbs.ClaimTask(task.Guid, "executor-id")
				Ω(err).ShouldNot(HaveOccurred())

				err = bbs.StartTask(task.Guid, "executor-id", "container-handle")
				Ω(err).ShouldNot(HaveOccurred())

				err = bbs.CompleteTask(task.Guid, true, "'cause I said so", "a result")
				Ω(err).ShouldNot(HaveOccurred())

				err = bbs.ResolvingTask(task.Guid)
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("should do nothing", func() {
				commenceWatching()

				bbs.ConvergeTask(timeToClaim, convergenceInterval)

				Consistently(desiredEvents).ShouldNot(Receive())
				Consistently(completedEvents).ShouldNot(Receive())
			})

			Context("when the run once has been resolving for > 30 seconds", func() {
				It("should put the Task back into the completed state", func() {
					timeProvider.IncrementBySeconds(convergenceIntervalInSeconds)
					commenceWatching()

					bbs.ConvergeTask(timeToClaim, convergenceInterval)

					var noticedOnce models.Task
					Eventually(completedEvents).Should(Receive(&noticedOnce))

					Ω(noticedOnce.Guid).Should(Equal(task.Guid))
					Ω(noticedOnce.State).Should(Equal(models.TaskStateCompleted))
					Ω(noticedOnce.UpdatedAt).Should(Equal(timeProvider.Time().UnixNano()))
				})
			})
		})
	})
})
