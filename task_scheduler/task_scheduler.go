package task_scheduler

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/executor/client"
	"github.com/cloudfoundry-incubator/rep/routes"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gosteno"
	"github.com/tedsuo/router"
)

const ServerCloseErrMsg = "use of closed network connection"

type TaskScheduler struct {
	callbackGenerator *router.RequestGenerator

	bbs            bbs.RepBBS
	logger         *gosteno.Logger
	stack          string
	client         client.Client
	inFlight       *sync.WaitGroup
	exitChan       chan struct{}
	terminatedChan chan struct{}
}

func New(
	callbackGenerator *router.RequestGenerator,
	bbs bbs.RepBBS,
	logger *gosteno.Logger,
	stack string,
	executorClient client.Client,
) *TaskScheduler {
	return &TaskScheduler{
		callbackGenerator: callbackGenerator,

		bbs:    bbs,
		logger: logger,
		stack:  stack,
		client: executorClient,

		inFlight: &sync.WaitGroup{},
	}
}

func (s *TaskScheduler) Run(readyChan chan struct{}) error {
	s.exitChan = make(chan struct{})
	s.terminatedChan = make(chan struct{})
	s.logger.Info("executor.watching-for-desired-task")

	go func() {
		tasks, stopChan, errChan := s.bbs.WatchForDesiredTask()

		if readyChan != nil {
			close(readyChan)
		}

		for {
			select {
			case err := <-errChan:
				s.logError("task-scheduler.watch-desired.restart", err)
				tasks, stopChan, errChan = s.bbs.WatchForDesiredTask()

			case task, ok := <-tasks:
				if !ok {
					err := errors.New("task channel closed. This is very unexpected, we did not intented to exit like this.")
					s.logError("task-scheduler.watch-desired.task-chan-closed", err)

					s.gracefulShutdown()
					close(s.terminatedChan)
					return
				}

				s.inFlight.Add(1)
				go func() {
					s.handleTaskRequest(task)
					s.inFlight.Done()
				}()

			case <-s.exitChan:
				s.gracefulShutdown()
				close(stopChan)
				close(s.terminatedChan)
				return
			}
		}
	}()
	return nil
}

func (s *TaskScheduler) Stop() {
	if s.exitChan != nil {
		close(s.exitChan)
		<-s.terminatedChan
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

	container, err := s.client.AllocateContainer(task.Guid, api.ContainerAllocationRequest{
		DiskMB:   task.DiskMB,
		MemoryMB: task.MemoryMB,
	})
	if err != nil {
		s.logError("task-scheduler.allocation-request.failed", err)
		return
	}

	s.sleepForARandomInterval()

	task, err = s.bbs.ClaimTask(task, container.ExecutorGuid)
	if err != nil {
		s.logError("task-scheduler.claim-task.failed", err)
		s.client.DeleteContainer(container.Guid)
		return
	}

	err = s.client.InitializeContainer(container.Guid, api.ContainerInitializationRequest{
		CpuPercent: task.CpuPercent,
		Log:        task.Log,
	})
	if err != nil {
		s.logError("task-scheduler.initialize-container-request.failed", err)
		s.client.DeleteContainer(container.Guid)
		return
	}

	task, err = s.bbs.StartTask(task, container.Guid)
	if err != nil {
		s.logError("task-scheduler.start-task.failed", err)
		s.client.DeleteContainer(container.Guid)
		return
	}

	callbackRequest, err := s.callbackGenerator.RequestForHandler(routes.TaskCompleted, router.Params{
		"guid": container.Guid,
	}, nil)
	if err != nil {
		s.logError("task-scheduler.callback-generator.failed", err)
	}

	err = s.client.Run(container.Guid, api.ContainerRunRequest{
		Actions:     task.Actions,
		CompleteURL: callbackRequest.URL.String(),
		Metadata:    task.ToJSON(),
	})
	if err != nil {
		s.logError("task-scheduler.run-actions.failed", err)
	}
}

func (s *TaskScheduler) logError(topic string, err error) {
	s.logger.Errord(map[string]interface{}{"error": err.Error()}, topic)
}

func (s *TaskScheduler) sleepForARandomInterval() {
	interval := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(100)
	time.Sleep(time.Duration(interval) * time.Millisecond)
}
