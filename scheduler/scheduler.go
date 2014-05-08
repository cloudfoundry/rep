package scheduler

import (
	"encoding/json"
	"fmt"
	"github.com/cloudfoundry-incubator/executor/client"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry-incubator/runtime-schema/models/executor_api"
	"github.com/cloudfoundry/gosteno"
)

const ServerCloseErrMsg = "use of closed network connection"

type Scheduler struct {
	bbs          bbs.ExecutorBBS
	logger       *gosteno.Logger
	stack        string
	client       client.Client
	listener     net.Listener
	address      string
	inFlight     *sync.WaitGroup
	completeChan chan executor_api.ContainerRunResult
}

func New(bbs bbs.ExecutorBBS, logger *gosteno.Logger, stack, schedulerAddress string, executorClient client.Client) *Scheduler {
	return &Scheduler{
		bbs:          bbs,
		logger:       logger,
		stack:        stack,
		client:       executorClient,
		address:      schedulerAddress,
		inFlight:     &sync.WaitGroup{},
		completeChan: make(chan executor_api.ContainerRunResult),
	}
}

func (s *Scheduler) startServer() {
	err := http.Serve(s.listener, s)
	if err != nil && err.Error() != ServerCloseErrMsg {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.server.failed")
	}
}

func (s *Scheduler) stopServer() {
	err := s.listener.Close()
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.server-close.failed")
	}
}

func (s *Scheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	responseBody, err := ioutil.ReadAll(r.Body)
	r.Body.Close()

	completeResp := executor_api.ContainerRunResult{}
	err = json.Unmarshal(responseBody, &completeResp)
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

func (s *Scheduler) Run(sigChan chan os.Signal, readyChan chan struct{}) error {
	s.logger.Info("executor.watching-for-desired-task")
	tasks, _, _ := s.bbs.WatchForDesiredTask()

	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	s.listener = listener

	if readyChan != nil {
		close(readyChan)
	}

	go s.startServer()

	for {
		select {
		case task := <-tasks:
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

		case sig := <-sigChan:
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				s.stopServer()
				s.inFlight.Wait()
				return nil
			}
		}
	}
}

func (s *Scheduler) handleRunCompletion(runResult executor_api.ContainerRunResult) {
	task := models.Task{}
	err := json.Unmarshal(runResult.Metadata, &task)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": fmt.Sprintf("Could not unmarshal metadata: %s", err),
		}, "game-scheduler.complete-callback-handler.failed")
		return
	}

	s.bbs.CompleteTask(&task, runResult.Failed, runResult.FailureReason, runResult.Result)
}

func (s *Scheduler) handleTaskRequest(task *models.Task) {
	var err error

	if task.Stack != s.stack {
		return
	}

	container, err := s.client.AllocateContainer(client.ContainerRequest{
		DiskMB:     task.DiskMB,
		MemoryMB:   task.MemoryMB,
		CpuPercent: task.CpuPercent,
	})
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.allocation-request.failed")
		return
	}

	s.sleepForARandomInterval()

	err = s.bbs.ClaimTask(task, container.ExecutorGuid)
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

	err = s.bbs.StartTask(task, container.Guid)
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

func (s *Scheduler) sleepForARandomInterval() {
	interval := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(100)
	time.Sleep(time.Duration(interval) * time.Millisecond)
}
