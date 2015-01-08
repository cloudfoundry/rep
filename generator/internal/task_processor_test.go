package internal_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep/generator/internal"
	"github.com/cloudfoundry-incubator/rep/generator/internal/fake_internal"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/storeadapter"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
)

const taskGuid = "my-guid"

var processor internal.TaskProcessor
var BBS *bbs.BBS
var etcdRunner *etcdstorerunner.ETCDClusterRunner
var etcdClient storeadapter.StoreAdapter

var _ = BeforeSuite(func() {
	etcdRunner = etcdstorerunner.NewETCDClusterRunner(5001+config.GinkgoConfig.ParallelNode, 1)
	etcdClient = etcdRunner.Adapter()
})

var _ = AfterSuite(func() {
	etcdRunner.Stop()
})

var _ = Describe("Task <-> Container table", func() {
	var (
		containerDelegate *fake_internal.FakeContainerDelegate
	)
	const (
		localCellID   = "a"
		otherCellID   = "w"
		sessionPrefix = "task-table-test"
	)

	BeforeEach(func() {
		etcdRunner.Stop()
		etcdRunner.Start()
		BBS = bbs.NewBBS(etcdClient, timeprovider.NewTimeProvider(), lagertest.NewTestLogger("test-bbs"))
		containerDelegate = new(fake_internal.FakeContainerDelegate)
		processor = internal.NewTaskProcessor(BBS, containerDelegate, localCellID)

		containerDelegate.DeleteContainerReturns(true)
		containerDelegate.StopContainerReturns(true)
		containerDelegate.RunContainerReturns(true)
	})

	itDeletesTheContainer := func(logger *lagertest.TestLogger) {
		It("deletes the container", func() {
			Ω(containerDelegate.DeleteContainerCallCount()).Should(Equal(1))
			_, containerGuid := containerDelegate.DeleteContainerArgsForCall(0)
			Ω(containerGuid).Should(Equal(taskGuid))
		})
	}

	itCompletesTheTaskWithFailure := func(reason string) func(*lagertest.TestLogger) {
		return func(logger *lagertest.TestLogger) {
			It("completes the task with failure", func() {
				task, err := BBS.TaskByGuid(taskGuid)
				Ω(err).ShouldNot(HaveOccurred())

				Ω(task.State).Should(Equal(models.TaskStateCompleted))
				Ω(task.Failed).Should(BeTrue())
				Ω(task.FailureReason).Should(Equal(reason))
			})
		}
	}

	itCompletesTheTaskAndDeletesTheContainer := func(logger *lagertest.TestLogger) {
		Context("when fetching the result succeeds", func() {
			BeforeEach(func() {
				containerDelegate.FetchContainerResultReturns("some-result", nil)

				containerDelegate.DeleteContainerStub = func(logger lager.Logger, guid string) bool {
					task, err := BBS.TaskByGuid(taskGuid)
					Ω(err).ShouldNot(HaveOccurred())

					Ω(task.State).Should(Equal(models.TaskStateCompleted))

					return true
				}
			})

			itDeletesTheContainer(logger)

			It("completes the task with the failure info and result", func() {
				task, err := BBS.TaskByGuid(taskGuid)
				Ω(err).ShouldNot(HaveOccurred())

				Ω(task.Failed).Should(Equal(true))
				Ω(task.FailureReason).Should(Equal("because"))
				Ω(task.Result).Should(Equal("some-result"))

				_, guid, filename := containerDelegate.FetchContainerResultArgsForCall(0)
				Ω(guid).Should(Equal(taskGuid))
				Ω(filename).Should(Equal("some-result-filename"))
			})
		})

		Context("when fetching the result fails", func() {
			disaster := errors.New("nope")

			BeforeEach(func() {
				containerDelegate.FetchContainerResultReturns("", disaster)
			})

			itCompletesTheTaskWithFailure("failed to fetch result")(logger)

			itDeletesTheContainer(logger)
		})
	}

	itSetsTheTaskToRunning := func(logger *lagertest.TestLogger) {
		It("transitions the task to the running state", func() {
			task, err := BBS.TaskByGuid(taskGuid)
			Ω(err).ShouldNot(HaveOccurred())

			Ω(task.State).Should(Equal(models.TaskStateRunning))
		})
	}

	itRunsTheContainer := func(logger *lagertest.TestLogger) {
		itSetsTheTaskToRunning(logger)

		It("runs the container", func() {
			Ω(containerDelegate.RunContainerCallCount()).Should(Equal(1))
			_, containerGuid := containerDelegate.RunContainerArgsForCall(0)
			Ω(containerGuid).Should(Equal(taskGuid))
		})

		Context("when running the container fails", func() {
			BeforeEach(func() {
				containerDelegate.RunContainerReturns(false)
			})

			itCompletesTheTaskWithFailure("failed to run container")(logger)
		})
	}

	itDoesNothing := func(logger *lagertest.TestLogger) {
		It("does not run the container", func() {
			Ω(containerDelegate.RunContainerCallCount()).Should(Equal(0))
		})

		It("does not stop the container", func() {
			Ω(containerDelegate.StopContainerCallCount()).Should(Equal(0))
		})

		It("does not delete the container", func() {
			Ω(containerDelegate.DeleteContainerCallCount()).Should(Equal(0))
		})
	}

	table := TaskTable{
		LocalCellID: localCellID,
		Logger:      lagertest.NewTestLogger(sessionPrefix),
		Rows: []Row{
			// container reserved
			Conceivable( // task deleted? (operator/etcd?)
				NewContainer(executor.StateReserved),
				nil,
				itDeletesTheContainer,
			),
			Expected( // container is reserved for a pending container
				NewContainer(executor.StateReserved),
				NewTask("", models.TaskStatePending),
				itRunsTheContainer,
			),
			Expected( // task is started before we run the container. it should eventually transition to initializing or be reaped if things really go wrong.
				NewContainer(executor.StateReserved),
				NewTask("a", models.TaskStateRunning),
				itDoesNothing,
			),
			Inconceivable( // state machine borked? no two cells should reserve the same task.
				NewContainer(executor.StateReserved),
				NewTask("w", models.TaskStateRunning),
				itDeletesTheContainer,
			),
			Conceivable( // if the Run call to the executor fails we complete the task with failure, and try to remove the reservation, but there's a time window.
				NewContainer(executor.StateReserved),
				NewTask("a", models.TaskStateCompleted),
				itDeletesTheContainer,
			),
			Inconceivable( // state machine borked?
				NewContainer(executor.StateReserved),
				NewTask("w", models.TaskStateCompleted),
				itDeletesTheContainer,
			),
			Conceivable( // caller is processing failure from Run call
				NewContainer(executor.StateReserved),
				NewTask("a", models.TaskStateResolving),
				itDeletesTheContainer,
			),
			Inconceivable( // state machine borked?
				NewContainer(executor.StateReserved),
				NewTask("w", models.TaskStateResolving),
				itDeletesTheContainer,
			),

			// container initializing
			Conceivable( // task deleted? (operator/etcd?)
				NewContainer(executor.StateInitializing),
				nil,
				itDeletesTheContainer,
			),
			Inconceivable( // task should be started before anyone tries to run
				NewContainer(executor.StateInitializing),
				NewTask("", models.TaskStatePending),
				itRunsTheContainer,
			),
			Expected( // task is running throughout initializing, completed, and running
				NewContainer(executor.StateInitializing),
				NewTask("a", models.TaskStateRunning),
				itDoesNothing,
			),
			Inconceivable( // state machine borked? no other cell should get this far.
				NewContainer(executor.StateInitializing),
				NewTask("w", models.TaskStateRunning),
				itDeletesTheContainer,
			),
			Inconceivable( // today there shouldn't be anything completing an initializing container.
				NewContainer(executor.StateInitializing),
				NewTask("a", models.TaskStateCompleted),
				itDeletesTheContainer,
			),
			Inconceivable( // state machine borked? no other cell should get this far.
				NewContainer(executor.StateInitializing),
				NewTask("w", models.TaskStateCompleted),
				itDeletesTheContainer,
			),
			Inconceivable( // today there shouldn't be anything completing an initializing container.
				NewContainer(executor.StateInitializing),
				NewTask("a", models.TaskStateResolving),
				itDeletesTheContainer,
			),
			Inconceivable( // state machine borked? no other cell should get this far.
				NewContainer(executor.StateInitializing),
				NewTask("w", models.TaskStateResolving),
				itDeletesTheContainer,
			),

			// container created
			Conceivable( // task deleted? (operator/etcd?)
				NewContainer(executor.StateCreated),
				nil,
				itDeletesTheContainer,
			),
			Inconceivable( // task should be started before anyone tries to run
				NewContainer(executor.StateCreated),
				NewTask("", models.TaskStatePending),
				itSetsTheTaskToRunning,
			),
			Expected( // task is running throughout initializing, completed, and running
				NewContainer(executor.StateCreated),
				NewTask("a", models.TaskStateRunning),
				itDoesNothing,
			),
			Inconceivable( // state machine borked? no other cell should get this far.
				NewContainer(executor.StateCreated),
				NewTask("w", models.TaskStateRunning),
				itDeletesTheContainer,
			),
			Inconceivable( // today there shouldn't be anything completing a still-active container.
				NewContainer(executor.StateCreated),
				NewTask("a", models.TaskStateCompleted),
				itDeletesTheContainer,
			),
			Inconceivable( // state machine borked? no other cell should get this far.
				NewContainer(executor.StateCreated),
				NewTask("w", models.TaskStateCompleted),
				itDeletesTheContainer,
			),
			Inconceivable( // container should be completed before anyone resolves it.
				NewContainer(executor.StateCreated),
				NewTask("a", models.TaskStateResolving),
				itDeletesTheContainer,
			),
			Inconceivable( // state machine borked? no other cell should get this far.
				NewContainer(executor.StateCreated),
				NewTask("w", models.TaskStateResolving),
				itDeletesTheContainer,
			),

			// container running
			Conceivable( // task deleted? (operator/etcd?)
				NewContainer(executor.StateRunning),
				nil,
				itDeletesTheContainer,
			),
			Inconceivable( // task should be started before anyone tries to run
				NewContainer(executor.StateRunning),
				NewTask("", models.TaskStatePending),
				itSetsTheTaskToRunning,
			),
			Expected( // task is running throughout initializing, completed, and running
				NewContainer(executor.StateRunning),
				NewTask("a", models.TaskStateRunning),
				itDoesNothing,
			),
			Inconceivable( // state machine borked? no other cell should get this far.
				NewContainer(executor.StateRunning),
				NewTask("w", models.TaskStateRunning),
				itDeletesTheContainer,
			),
			Inconceivable( // today there shouldn't be anything completing a still-active container.
				NewContainer(executor.StateRunning),
				NewTask("a", models.TaskStateCompleted),
				itDeletesTheContainer,
			),
			Inconceivable( // state machine borked? no other cell should get this far.
				NewContainer(executor.StateRunning),
				NewTask("w", models.TaskStateCompleted),
				itDeletesTheContainer,
			),
			Inconceivable( // container should be completed before anyone resolves it.
				NewContainer(executor.StateRunning),
				NewTask("a", models.TaskStateResolving),
				itDeletesTheContainer,
			),
			Inconceivable( // state machine borked? no other cell should get this far.
				NewContainer(executor.StateRunning),
				NewTask("w", models.TaskStateResolving),
				itDeletesTheContainer,
			),

			// container completed
			Conceivable( // task deleted? (operator/etcd?)
				NewCompletedContainer(),
				nil,
				itDeletesTheContainer,
			),
			Inconceivable( // task should be walked through lifecycle by the time we get here
				NewCompletedContainer(),
				NewTask("", models.TaskStatePending),
				itCompletesTheTaskWithFailure("invalid state transition"),
			),
			Expected( // container completed; complete the task with its result
				NewCompletedContainer(),
				NewTask("a", models.TaskStateRunning),
				itCompletesTheTaskAndDeletesTheContainer,
			),
			Inconceivable( // state machine borked? no other cell should get this far.
				NewCompletedContainer(),
				NewTask("w", models.TaskStateRunning),
				itDeletesTheContainer,
			),
			Conceivable( // may have completed the task and then failed to delete the container
				NewCompletedContainer(),
				NewTask("a", models.TaskStateCompleted),
				itDeletesTheContainer,
			),
			Inconceivable( // state machine borked? no other cell should get this far.
				NewCompletedContainer(),
				NewTask("w", models.TaskStateCompleted),
				itDeletesTheContainer,
			),
			Conceivable( // may have completed the task and then failed to delete the container, and someone started processing the completion
				NewCompletedContainer(),
				NewTask("a", models.TaskStateResolving),
				itDeletesTheContainer,
			),
			Inconceivable( // state machine borked? no other cell should get this far.
				NewCompletedContainer(),
				NewTask("w", models.TaskStateResolving),
				itDeletesTheContainer,
			),
		},
	}

	table.Test()
})

type TaskTable struct {
	LocalCellID string
	Processor   *internal.TaskProcessor
	Logger      *lagertest.TestLogger
	Rows        []Row
}

func (t *TaskTable) Test() {
	for _, row := range t.Rows {
		row := row

		Context(row.ContextDescription(), func() {
			row.Test(t.Logger)
		})
	}
}

type Row interface {
	ContextDescription() string
	Test(*lagertest.TestLogger)
}

type TaskTest func(*lagertest.TestLogger)

type TaskRow struct {
	Container executor.Container
	Task      *models.Task
	TestFunc  TaskTest
}

func (e TaskRow) Test(logger *lagertest.TestLogger) {
	BeforeEach(func() {
		if e.Task != nil {
			walkToState(logger, BBS, *e.Task)
		}
	})

	JustBeforeEach(func() {
		processor.Process(logger, e.Container)
	})

	e.TestFunc(logger)
}

func (t TaskRow) ContextDescription() string {
	return "when the container is " + t.containerDescription() + " and the task is " + t.taskDescription()
}

func (t TaskRow) containerDescription() string {
	return string(t.Container.State)
}

func (t TaskRow) taskDescription() string {
	if t.Task == nil {
		return "missing"
	}

	msg := t.Task.State.String()
	if t.Task.CellID != "" {
		msg += " on '" + t.Task.CellID + "'"
	}

	return msg
}

func Expected(container executor.Container, task *models.Task, test TaskTest) Row {
	expectedTest := func(logger *lagertest.TestLogger) {
		test(logger)

		//		It("does not log that it's inconceivable", func() {
		//			Ω(logger).ShouldNot(gbytes.Say("inconceivable-state"))
		//		})
	}

	return TaskRow{container, task, TaskTest(expectedTest)}
}

func Conceivable(container executor.Container, task *models.Task, test TaskTest) Row {
	conceivableTest := func(logger *lagertest.TestLogger) {
		test(logger)

		//		It("does not log that it's inconceivable", func() {
		//			Ω(logger).ShouldNot(gbytes.Say("inconceivable-state"))
		//		})
	}

	return TaskRow{container, task, TaskTest(conceivableTest)}
}

func Inconceivable(container executor.Container, task *models.Task, test TaskTest) Row {
	inconceivableTest := func(logger *lagertest.TestLogger) {
		test(logger)

		//		It("logs that it's inconceivable", func() {
		//			Ω(logger).Should(gbytes.Say("inconceivable-state"))
		//		})
	}

	return TaskRow{container, task, TaskTest(inconceivableTest)}
}

func NewContainer(containerState executor.State) executor.Container {
	return executor.Container{
		Guid:  taskGuid,
		State: containerState,
	}
}

func NewCompletedContainer() executor.Container {
	return executor.Container{
		Guid:  taskGuid,
		State: executor.StateCompleted,

		RunResult: executor.ContainerRunResult{
			Failed:        true,
			FailureReason: "because",
		},
	}
}

func NewTask(cellID string, taskState models.TaskState) *models.Task {
	return &models.Task{
		TaskGuid:   taskGuid,
		CellID:     cellID,
		State:      taskState,
		ResultFile: "some-result-filename",
		Domain:     "domain",
		Stack:      "stack",
		Action:     &models.RunAction{Path: "ls"},
	}
}

func walkToState(logger lager.Logger, BBS *bbs.BBS, task models.Task) {
	var currentState models.TaskState
	desiredState := task.State
	for desiredState != currentState {
		currentState = advanceState(logger, BBS, task, currentState)
	}
}

func advanceState(logger lager.Logger, BBS *bbs.BBS, task models.Task, currentState models.TaskState) models.TaskState {
	switch currentState {
	case models.TaskStateInvalid:
		err := BBS.DesireTask(logger, task)
		Ω(err).ShouldNot(HaveOccurred())
		return models.TaskStatePending

	case models.TaskStatePending:
		changed, err := BBS.StartTask(logger, task.TaskGuid, task.CellID)
		Ω(err).ShouldNot(HaveOccurred())
		Ω(changed).Should(BeTrue())
		return models.TaskStateRunning

	case models.TaskStateRunning:
		err := BBS.CompleteTask(logger, task.TaskGuid, task.CellID, true, "reason", "result")
		Ω(err).ShouldNot(HaveOccurred())
		return models.TaskStateCompleted

	case models.TaskStateCompleted:
		err := BBS.ResolvingTask(logger, task.TaskGuid)
		Ω(err).ShouldNot(HaveOccurred())
		return models.TaskStateResolving

	default:
		panic("not a thing.")
	}
}
