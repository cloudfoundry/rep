package task_scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/executor/client"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gosteno"
)

const ServerCloseErrMsg = "use of closed network connection"

type TaskScheduler struct {
	bbs            bbs.RepBBS
	logger         *gosteno.Logger
	stack          string
	client         client.Client
	listener       net.Listener
	address        string
	inFlight       *sync.WaitGroup
	completeChan   chan client.ContainerRunResult
	exitChan       chan struct{}
	terminatedChan chan struct{}
}

func New(bbs bbs.RepBBS, logger *gosteno.Logger, stack, schedulerAddress string, executorClient client.Client) *TaskScheduler {
	return &TaskScheduler{
		bbs:          bbs,
		logger:       logger,
		stack:        stack,
		client:       executorClient,
		address:      schedulerAddress,
		inFlight:     &sync.WaitGroup{},
		completeChan: make(chan client.ContainerRunResult),
	}
}

func (s *TaskScheduler) startServer() {
	err := http.Serve(s.listener, s)
	if err != nil && err.Error() != ServerCloseErrMsg {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.server.failed")
	}
}

func (s *TaskScheduler) stopServer() {
	err := s.listener.Close()
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.server-close.failed")
	}
}

func (s *TaskScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	responseBody, err := ioutil.ReadAll(r.Body)
	r.Body.Close()

	completeResp, err := client.NewContainerRunResultFromJSON(responseBody)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": fmt.Sprintf("Could not unmarshal response: %s", err),
		}, "game-scheduler.complete-callback-handler.failed")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	s.completeChan <- completeResp
	w.WriteHeader(http.StatusOK)
}

func (s *TaskScheduler) Run(readyChan chan struct{}) error {
	s.exitChan = make(chan struct{})
	s.terminatedChan = make(chan struct{})
	s.logger.Info("executor.watching-for-desired-task")

	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	s.listener = listener

	go s.startServer()

	go func() {
		tasks, stopChan, errChan := s.bbs.WatchForDesiredTask()

		if readyChan != nil {
			close(readyChan)
		}

		for {
			select {
			case err := <-errChan:
				s.logger.Errord(map[string]interface{}{
					"error": err.Error(),
				}, "game-scheduler.watch-desired.restart")
				tasks, stopChan, errChan = s.bbs.WatchForDesiredTask()

			case task, ok := <-tasks:
				if !ok {
					s.logger.Errord(map[string]interface{}{
						"error": errors.New("task channel closed. This is very unexpected, we did not intented to exit like this."),
					}, "game-scheduler.watch-desired.task-chan-closed")
					s.gracefulShutdown()
					close(s.terminatedChan)
					return
				}
				s.inFlight.Add(1)
				go func() {
					s.handleTaskRequest(task)
					s.inFlight.Done()
				}()

			case runResult := <-s.completeChan:
				s.inFlight.Add(1)
				go func() {
					s.handleRunCompletion(runResult)
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
	s.stopServer()
	s.inFlight.Wait()
}

func (s *TaskScheduler) handleRunCompletion(runResult client.ContainerRunResult) {
	task := models.Task{}
	err := json.Unmarshal(runResult.Metadata, &task)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": fmt.Sprintf("Could not unmarshal metadata: %s", err),
		}, "game-scheduler.complete-callback-handler.failed")
		return
	}

	s.bbs.CompleteTask(task, runResult.Failed, runResult.FailureReason, runResult.Result)
}

func (s *TaskScheduler) handleTaskRequest(task models.Task) {
	var err error

	if task.Stack != s.stack {
		return
	}

	container, err := s.client.AllocateContainer(client.ContainerRequest{
		DiskMB:     task.DiskMB,
		MemoryMB:   task.MemoryMB,
		CpuPercent: task.CpuPercent,
		LogConfig:  task.Log,
	})
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.allocation-request.failed")
		return
	}

	s.sleepForARandomInterval()

	task, err = s.bbs.ClaimTask(task, container.ExecutorGuid)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.claim-task.failed")
		s.client.DeleteContainer(container.Guid)
		return
	}

	err = s.client.InitializeContainer(container.Guid)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.initialize-container-request.failed")
		s.client.DeleteContainer(container.Guid)
		return
	}

	task, err = s.bbs.StartTask(task, container.Guid)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.start-task.failed")
		s.client.DeleteContainer(container.Guid)
		return
	}

	err = s.client.Run(container.Guid, client.RunRequest{
		Actions:       task.Actions,
		CompletionURL: fmt.Sprintf("http://%s/complete/%s", s.address, container.Guid),
		Metadata:      task.ToJSON(),
	})
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.run-actions.failed")
	}
}

func (s *TaskScheduler) sleepForARandomInterval() {
	interval := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(100)
	time.Sleep(time.Duration(interval) * time.Millisecond)
}
