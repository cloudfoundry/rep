package task_scheduler

import (
	"errors"
	"math/rand"
	"os"
	"sync"
	"time"

	executorapi "github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/rep/routes"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
	"github.com/tedsuo/rata"
)

const ServerCloseErrMsg = "use of closed network connection"

var random = rand.New(rand.NewSource(time.Now().UnixNano()))

type TaskScheduler struct {
	callbackGenerator *rata.RequestGenerator

	executorID string
	bbs        bbs.RepBBS
	logger     lager.Logger
	stack      string
	client     executorapi.Client
	inFlight   *sync.WaitGroup
}

func New(
	executorID string,
	callbackGenerator *rata.RequestGenerator,
	bbs bbs.RepBBS,
	logger lager.Logger,
	stack string,
	executorClient executorapi.Client,
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

	taskLog := s.logger.Session("task-request")

	_, err = s.client.AllocateContainer(task.Guid, executorapi.ContainerAllocationRequest{
		DiskMB:   task.DiskMB,
		MemoryMB: task.MemoryMB,
	})
	if err != nil {
		taskLog.Error("failed-to-allocate", err)
		return
	}

	s.sleepForARandomInterval()

	err = s.bbs.ClaimTask(task.Guid, s.executorID)
	if err != nil {
		taskLog.Error("failed-to-claim-task", err)
		s.client.DeleteContainer(task.Guid)
		return
	}

	container, err := s.client.InitializeContainer(task.Guid, executorapi.ContainerInitializationRequest{
		CpuPercent: task.CpuPercent,
		Log: executorapi.LogConfig{
			Guid:       task.Log.Guid,
			SourceName: task.Log.SourceName,
		},
	})
	if err != nil {
		taskLog.Error("failed-to-initialize-container", err)
		s.client.DeleteContainer(task.Guid)
		s.markTaskAsFailed(taskLog, task.Guid, err)
		return
	}

	err = s.bbs.StartTask(task.Guid, s.executorID, container.ContainerHandle)
	if err != nil {
		taskLog.Error("failed-to-mark-task-started", err)
		s.client.DeleteContainer(task.Guid)
		return
	}

	callbackRequest, err := s.callbackGenerator.CreateRequest(routes.TaskCompleted, rata.Params{
		"guid": task.Guid,
	}, nil)
	if err != nil {
		taskLog.Error("failed-to-generate-callback-request", err)
		return
	}

	err = s.client.Run(task.Guid, executorapi.ContainerRunRequest{
		Actions:     task.Actions,
		CompleteURL: callbackRequest.URL.String(),
	})
	if err != nil {
		taskLog.Error("failed-to-run-actions", err)
		return
	}

	return
}

func (s *TaskScheduler) markTaskAsFailed(taskLog lager.Logger, taskGuid string, err error) {
	err = s.bbs.CompleteTask(taskGuid, true, "Failed to initialize container - "+err.Error(), "")
	if err != nil {
		taskLog.Error("failed-to-mark-task-failed", err)
	}
}

func (s *TaskScheduler) sleepForARandomInterval() {
	interval := random.Intn(1000)
	time.Sleep(time.Duration(interval) * time.Millisecond)
}
