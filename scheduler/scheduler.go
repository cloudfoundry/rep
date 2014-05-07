package scheduler

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	"github.com/tedsuo/router"
)

const ServerCloseErrMsg = "use of closed network connection"

type Scheduler struct {
	bbs          bbs.ExecutorBBS
	logger       *gosteno.Logger
	executorURL  string
	stack        string
	reqGen       *router.RequestGenerator
	client       http.Client
	listener     net.Listener
	address      string
	inFlight     *sync.WaitGroup
	completeChan chan executor_api.ContainerRunResult
}

func New(bbs bbs.ExecutorBBS, logger *gosteno.Logger, stack string, schedulerAddress, executorURL string) *Scheduler {
	return &Scheduler{
		bbs:          bbs,
		logger:       logger,
		executorURL:  executorURL,
		stack:        stack,
		reqGen:       router.NewRequestGenerator(executorURL, executor_api.Routes),
		client:       http.Client{},
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

	container, succeeded := s.allocateContainer(task)
	if !succeeded {
		return
	}

	s.sleepForARandomInterval()

	err = s.bbs.ClaimTask(task, container.ExecutorGuid)
	if err != nil {
		s.deleteAllocation(container.Guid)
		return
	}

	succeeded = s.initializeContainer(container.Guid)
	if !succeeded {
		s.deleteAllocation(container.Guid)
		return
	}

	err = s.bbs.StartTask(task, container.Guid)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.start-task.failed")
		s.deleteAllocation(container.Guid)
		return
	}

	s.runActions(container.Guid, task)
}

func (s *Scheduler) allocateContainer(task *models.Task) (container executor_api.Container, succeeded bool) {
	reqBody, err := json.Marshal(executor_api.ContainerAllocationRequest{
		MemoryMB:        task.MemoryMB,
		DiskMB:          task.DiskMB,
		CpuPercent:      task.CpuPercent,
		FileDescriptors: task.FileDescriptors,
	})
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": fmt.Sprintf("Could not marshal json: %s", err),
		}, "game-scheduler.allocation-request-json.failed")
	}

	req, err := s.reqGen.RequestForHandler(executor_api.AllocateContainer, nil, bytes.NewBuffer(reqBody))
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.allocation-request-generation.failed")
		return
	}
	req.Header.Set("Content-Type", "application/json")

	response, err := s.client.Do(req)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.allocation-request.failed")
		return
	}

	if response.StatusCode == http.StatusRequestEntityTooLarge {
		s.logger.Infod(map[string]interface{}{
			"error": "Executor out of resources",
		}, "game-scheduler.allocate-container.full")
		return
	}

	if response.StatusCode != http.StatusCreated {
		s.logger.Errord(map[string]interface{}{
			"error": fmt.Sprintf("Executor responded with status code %d", response.StatusCode),
		}, "game-scheduler.allocate-container.failed")
		return
	}

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": fmt.Sprintf("Could not read response body: %s", err),
		}, "game-scheduler.allocate-container.failed")
		return
	}

	response.Body.Close()

	container = executor_api.Container{}
	err = json.Unmarshal(responseBody, &container)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": fmt.Sprintf("Could not unmarshal json: %s", err),
		}, "game-scheduler.allocate-container.failed")
		return
	}

	return container, true
}

func (s *Scheduler) initializeContainer(allocationGuid string) bool {
	req, err := s.reqGen.RequestForHandler(executor_api.InitializeContainer, router.Params{"guid": allocationGuid}, nil)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.initialize-request-generation.failed")
		return false
	}

	response, err := s.client.Do(req)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.initialize-container-request.failed")
		return false
	}
	if response.StatusCode != http.StatusCreated {
		s.logger.Errord(map[string]interface{}{
			"error": fmt.Sprintf("Executor responded with status code %d", response.StatusCode),
		}, "game-scheduler.initialize-container.failed")
		return false
	}
	return true
}

func (s *Scheduler) runActions(allocationGuid string, task *models.Task) {
	reqBody, err := json.Marshal(executor_api.ContainerRunRequest{
		Actions:     task.Actions,
		Metadata:    task.ToJSON(),
		CompleteURL: "http://" + s.address + "/complete/" + allocationGuid,
	})

	req, err := s.reqGen.RequestForHandler(
		executor_api.RunActions,
		router.Params{"guid": allocationGuid},
		bytes.NewReader(reqBody),
	)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.run-actions-request-generation.failed")
		return
	}
	req.Header.Set("Content-Type", "application/json")

	response, err := s.client.Do(req)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.run-actions-request.failed")
	}
	if response.StatusCode != http.StatusOK {
		s.logger.Errord(map[string]interface{}{
			"error": fmt.Sprintf("Executor responded with status code %d", response.StatusCode),
		}, "game-scheduler.run-actions.failed")
	}
}

func (s *Scheduler) deleteAllocation(allocationGuid string) {
	req, err := s.reqGen.RequestForHandler(executor_api.DeleteContainer, router.Params{"guid": allocationGuid}, nil)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.delete-container-request.failed")
		return
	}

	response, err := s.client.Do(req)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.delete-contatiner-request.failed")
		return
	}

	if response.StatusCode != http.StatusOK {
		s.logger.Errord(map[string]interface{}{
			"error": fmt.Sprintf("Executor responded with status code %d", response.StatusCode),
		}, "game-scheduler.delete-container.failed")
	}
}

func (s *Scheduler) sleepForARandomInterval() {
	interval := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(100)
	time.Sleep(time.Duration(interval) * time.Millisecond)
}
