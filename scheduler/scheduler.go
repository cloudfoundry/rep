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
	"github.com/cloudfoundry/gosteno"
)

const ServerCloseErrMsg = "use of closed network connection"

type Scheduler struct {
	bbs          bbs.ExecutorBBS
	logger       *gosteno.Logger
	executorURL  string
	client       http.Client
	listener     net.Listener
	address      string
	inFlight     *sync.WaitGroup
	completeChan chan models.JobResponse
}

func New(bbs bbs.ExecutorBBS, logger *gosteno.Logger, schedulerAddress, executorURL string) *Scheduler {
	return &Scheduler{
		bbs:          bbs,
		logger:       logger,
		executorURL:  executorURL,
		client:       http.Client{},
		address:      schedulerAddress,
		inFlight:     &sync.WaitGroup{},
		completeChan: make(chan models.JobResponse),
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
	completeResp := models.JobResponse{}
	err = json.Unmarshal(responseBody, &completeResp)
	if err != nil {
		panic(err)
	}
	s.completeChan <- completeResp
	w.WriteHeader(http.StatusOK)
}

func (s *Scheduler) Run(sigChan chan os.Signal, readyChan chan struct{}) error {
	s.logger.Info("executor.watching-for-desired-task")
	tasks, _, _ := s.bbs.WatchForDesiredTask()

	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		panic(err)
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

		case jobResponse := <-s.completeChan:
			s.inFlight.Add(1)
			go func() {
				s.handleJobCompletion(jobResponse)
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

func (s *Scheduler) handleJobCompletion(jobResponse models.JobResponse) {
	task := models.Task{}
	err := json.Unmarshal(jobResponse.Metadata, &task)
	if err != nil {
		panic(err)
	}

	s.bbs.CompleteTask(&task, jobResponse.Failed, jobResponse.FailureReason, jobResponse.Result)
}

func (s *Scheduler) handleTaskRequest(task *models.Task) {
	var err error
	allocationResp, succeeded := s.reserveResources(task)
	if !succeeded {
		return
	}

	s.sleepForARandomInterval()

	err = s.bbs.ClaimTask(task, allocationResp.ExecutorGuid)
	if err != nil {
		s.deleteAllocation(allocationResp.AllocationGuid)
		return
	}

	succeeded = s.createContainer(allocationResp.AllocationGuid)
	if !succeeded {
		s.deleteAllocation(allocationResp.AllocationGuid)
		return
	}

	err = s.bbs.StartTask(task, allocationResp.AllocationGuid)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.start-task.failed")
		s.deleteAllocation(allocationResp.AllocationGuid)
		return
	}

	s.runActions(allocationResp.AllocationGuid, task)
}

func (s *Scheduler) reserveResources(task *models.Task) (allocationResp models.ResourceAllocationResponse, succeeded bool) {
	reqBody, err := json.Marshal(models.ResourceAllocationRequest{
		MemoryMB:        task.MemoryMB,
		DiskMB:          task.DiskMB,
		CpuPercent:      task.CpuPercent,
		FileDescriptors: task.FileDescriptors,
	})
	if err != nil {
		panic(err)
	}

	response, err := s.client.Post(s.executorURL+"/resource_allocations", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.reserve-resource-allocation-request.failed")
		return
	}

	if response.StatusCode == http.StatusRequestEntityTooLarge {
		s.logger.Infod(map[string]interface{}{
			"error": "Executor out of resources",
		}, "game-scheduler.reserve-resource-allocation.full")
		return
	}

	if response.StatusCode != http.StatusCreated {
		s.logger.Errord(map[string]interface{}{
			"error": fmt.Sprintf("Executor responded with status code %d", response.StatusCode),
		}, "game-scheduler.reserve-resource-allocation.failed")
		return
	}

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		panic(err)
	}

	allocationResp = models.ResourceAllocationResponse{}
	err = json.Unmarshal(responseBody, &allocationResp)
	if err != nil {
		panic(err)
	}

	return allocationResp, true
}

func (s *Scheduler) createContainer(allocationGuid string) bool {
	response, err := s.client.Post(s.executorURL+"/resource_allocations/"+allocationGuid+"/container", "", nil)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.create-container-request.failed")
		return false
	}
	if response.StatusCode != http.StatusCreated {
		s.logger.Errord(map[string]interface{}{
			"error": fmt.Sprintf("Executor responded with status code %d", response.StatusCode),
		}, "game-scheduler.create-container.failed")
		return false
	}
	return true
}

func (s *Scheduler) runActions(allocationGuid string, task *models.Task) {
	reqBody, err := json.Marshal(models.JobRequest{
		Actions:       task.Actions,
		Metadata:      task.ToJSON(),
		CompletionURL: "http://" + s.address + "/complete/" + allocationGuid,
	})
	response, err := s.client.Post(s.executorURL+"/resource_allocations/"+allocationGuid+"/actions", "application/json", bytes.NewReader(reqBody))
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
	req, err := http.NewRequest("DELETE", s.executorURL+"/resource_allocations/"+allocationGuid, nil)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.delete-resource-allocation-request.failed")
		return
	}

	response, err := s.client.Do(req)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "game-scheduler.delete-resource-allocation-request.failed")
		return
	}

	if response.StatusCode != http.StatusOK {
		s.logger.Errord(map[string]interface{}{
			"error": fmt.Sprintf("Executor responded with status code %d", response.StatusCode),
		}, "game-scheduler.delete-resource-allocation.failed")
	}
}

func (s *Scheduler) sleepForARandomInterval() {
	interval := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(100)
	time.Sleep(time.Duration(interval) * time.Millisecond)
}
