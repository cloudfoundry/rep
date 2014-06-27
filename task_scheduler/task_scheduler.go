package task_scheduler

import (
	"errors"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/executor/client"
	"github.com/cloudfoundry-incubator/rep/routes"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gosteno"
	"github.com/tedsuo/rata"
)

const ServerCloseErrMsg = "use of closed network connection"

type TaskScheduler struct {
	callbackGenerator *rata.RequestGenerator

	executorID string
	bbs        bbs.RepBBS
	logger     *gosteno.Logger
	stack      string
	client     client.Client
	inFlight   *sync.WaitGroup
}

func New(
	executorID string,
	callbackGenerator *rata.RequestGenerator,
	bbs bbs.RepBBS,
	logger *gosteno.Logger,
	stack string,
	executorClient client.Client,
) *TaskScheduler {
	return &TaskScheduler{
		executorID:        executorID,
		callbackGenerator: callbackGenerator,

		bbs:    bbs,
		logger: logger,
		stack:  stack,
		client: executorClient,

		inFlight: &sync.WaitGroup{},
	}
}

func (s *TaskScheduler) Run(signals <-chan os.Signal, readyChan chan<- struct{}) error {
	tasks, stopChan, errChan := s.bbs.WatchForDesiredTask()
	s.logger.Info("rep.watching-for-desired-task")

	close(readyChan)

	for {
		select {
		case err := <-errChan:
			s.logError("task-scheduler.watch-desired.restart", err)

			time.Sleep(3 * time.Second)

			tasks, stopChan, errChan = s.bbs.WatchForDesiredTask()

		case task, ok := <-tasks:
			if !ok {
				err := errors.New("task channel closed. This is very unexpected, we did not intented to exit like this.")
				s.logError("task-scheduler.watch-desired.task-chan-closed", err)

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

	_, err = s.client.AllocateContainer(task.Guid, api.ContainerAllocationRequest{
		DiskMB:   task.DiskMB,
		MemoryMB: task.MemoryMB,
	})
	if err != nil {
		s.logError("task-scheduler.allocation-request.failed", err)
		return
	}

	s.sleepForARandomInterval()

	err = s.bbs.ClaimTask(task.Guid, s.executorID)
	if err != nil {
		s.logError("task-scheduler.claim-task.failed", err)
		s.client.DeleteContainer(task.Guid)
		return
	}

	container, err := s.client.InitializeContainer(task.Guid, api.ContainerInitializationRequest{
		CpuPercent: task.CpuPercent,
		Log:        task.Log,
	})
	if err != nil {
		s.logError("task-scheduler.initialize-container-request.failed", err)
		s.client.DeleteContainer(task.Guid)
		s.markTaskAsFailed(task.Guid, err)
		return
	}

	err = s.bbs.StartTask(task.Guid, s.executorID, container.ContainerHandle)
	if err != nil {
		s.logError("task-scheduler.start-task.failed", err)
		s.client.DeleteContainer(task.Guid)
		return
	}

	callbackRequest, err := s.callbackGenerator.CreateRequest(routes.TaskCompleted, rata.Params{
		"guid": task.Guid,
	}, nil)
	if err != nil {
		s.logError("task-scheduler.callback-generator.failed", err)
	}

	err = s.client.Run(task.Guid, api.ContainerRunRequest{
		Actions:     task.Actions,
		CompleteURL: callbackRequest.URL.String(),
	})
	if err != nil {
		s.logError("task-scheduler.run-actions.failed", err)
	}
}

func (s *TaskScheduler) markTaskAsFailed(taskGuid string, err error) {
	err = s.bbs.CompleteTask(taskGuid, true, "Failed to initialize container - "+err.Error(), "")
	if err != nil {
		s.logError("task-scheduler.mark-task-as-failed.failed", err)
	}
}

func (s *TaskScheduler) logError(topic string, err error) {
	s.logger.Errord(map[string]interface{}{"error": err.Error()}, topic)
}

func (s *TaskScheduler) sleepForARandomInterval() {
	interval := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(100)
	time.Sleep(time.Duration(interval) * time.Millisecond)
}
