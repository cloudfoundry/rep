package task_scheduler

import (
	"errors"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep/routes"
	"github.com/cloudfoundry-incubator/rep/tallyman"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
	"github.com/tedsuo/rata"
)

const (
	ServerCloseErrMsg    = "use of closed network connection"
	MaxClaimWaitInMillis = 1000
)

var random = rand.New(rand.NewSource(time.Now().UnixNano()))

type TaskScheduler struct {
	callbackGenerator *rata.RequestGenerator

	executorID string
	bbs        bbs.RepBBS
	logger     lager.Logger
	stack      string
	client     executor.Client
	inFlight   *sync.WaitGroup
}

func New(
	executorID string,
	callbackGenerator *rata.RequestGenerator,
	bbs bbs.RepBBS,
	logger lager.Logger,
	stack string,
	executorClient executor.Client,
) *TaskScheduler {
	return &TaskScheduler{
		executorID:        executorID,
		callbackGenerator: callbackGenerator,

		bbs:    bbs,
		logger: logger.Session("task-scheduler"),
		stack:  stack,
		client: executorClient,

		inFlight: &sync.WaitGroup{},
	}
}

func (s *TaskScheduler) Run(signals <-chan os.Signal, readyChan chan<- struct{}) error {
	watchLog := s.logger.Session("watching")

	tasks, stopChan, errChan := s.bbs.WatchForDesiredTask()

	watchLog.Info("started")

	close(readyChan)

	for {
		select {
		case err := <-errChan:
			watchLog.Error("failed", err)

			time.Sleep(3 * time.Second)

			tasks, stopChan, errChan = s.bbs.WatchForDesiredTask()

		case task, ok := <-tasks:
			if !ok {
				err := errors.New("task channel closed. This is very unexpected, we did not intented to exit like this.")

				watchLog.Error("task-channel-closed", err)

				s.gracefulShutdown()
				return nil
			}

			s.inFlight.Add(1)
			go func() {
				defer s.inFlight.Done()
				s.handleTaskRequest(task)
			}()

		case <-signals:
			s.gracefulShutdown()
			close(stopChan)
			return nil
		}
	}
}

func (s *TaskScheduler) gracefulShutdown() {
	s.inFlight.Wait()
}

func (s *TaskScheduler) handleTaskRequest(task models.Task) {
	var err error

	if task.Stack != s.stack {
		return
	}

	taskLog := s.logger.Session("task-request", lager.Data{"taskGuid": task.TaskGuid})

	callbackRequest, err := s.callbackGenerator.CreateRequest(routes.TaskCompleted, rata.Params{
		"guid": task.TaskGuid,
	}, nil)
	if err != nil {
		taskLog.Error("failed-to-generate-callback-request", err)
		return
	}

	taskLog.Info("allocating-container")
	_, err = s.client.AllocateContainer(executor.Container{
		Guid: task.TaskGuid,

		Tags: executor.Tags{tallyman.LifecycleTag: tallyman.TaskLifecycle},

		DiskMB:     task.DiskMB,
		MemoryMB:   task.MemoryMB,
		CPUWeight:  task.CPUWeight,
		RootFSPath: task.RootFSPath,
		Log: executor.LogConfig{
			Guid:       task.Log.Guid,
			SourceName: task.Log.SourceName,
		},

		Actions:     task.Actions,
		CompleteURL: callbackRequest.URL.String(),
	})
	if err != nil {
		taskLog.Error("failed-to-allocate-container", err)
		return
	}
	taskLog.Info("successfully-allocated-container")

	s.sleepForARandomInterval()

	taskLog.Info("claiming-task", lager.Data{"executorID": s.executorID})
	err = s.bbs.ClaimTask(task.TaskGuid, s.executorID)
	if err != nil {
		taskLog.Info("failed-to-claim-task", lager.Data{"error": err.Error()})
		s.client.DeleteContainer(task.TaskGuid)
		return
	}
	taskLog.Info("successfully-claimed-task")

	taskLog.Info("running-task")

	err = s.client.RunContainer(task.TaskGuid)
	if err != nil {
		taskLog.Error("failed-to-run-task", err)
		s.client.DeleteContainer(task.TaskGuid)
		s.markTaskAsFailed(taskLog, task.TaskGuid, err)
		return
	}

	taskLog.Info("successfully-ran-task")

	taskLog.Info("starting-task")
	err = s.bbs.StartTask(task.TaskGuid, s.executorID)
	if err != nil {
		taskLog.Error("failed-to-mark-task-started", err)
		s.client.DeleteContainer(task.TaskGuid)
		return
	}
	taskLog.Info("successfully-started-task")

	return
}

func (s *TaskScheduler) markTaskAsFailed(taskLog lager.Logger, taskGuid string, err error) {
	taskLog.Info("complete-task")
	err = s.bbs.CompleteTask(taskGuid, true, "failed to run container - "+err.Error(), "")
	if err != nil {
		taskLog.Error("failed-to-complete-task", err)
	}
	taskLog.Info("successfully-completed-task")
}

func (s *TaskScheduler) sleepForARandomInterval() {
	interval := random.Intn(MaxClaimWaitInMillis)
	time.Sleep(time.Duration(interval) * time.Millisecond)
}
