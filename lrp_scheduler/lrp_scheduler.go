package lrp_scheduler

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/executor/client"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gosteno"
)

type LrpScheduler struct {
	stack          string
	bbs            bbs.RepBBS
	logger         *gosteno.Logger
	client         client.Client
	inFlight       *sync.WaitGroup
	exitChan       chan struct{}
	terminatedChan chan struct{}
}

func New(bbs bbs.RepBBS, logger *gosteno.Logger, stack string, executorClient client.Client) *LrpScheduler {
	return &LrpScheduler{
		bbs:    bbs,
		logger: logger,
		stack:  stack,
		client: executorClient,

		inFlight: &sync.WaitGroup{},
	}
}

func (s *LrpScheduler) Run(readyChan chan struct{}) {
	s.terminatedChan = make(chan struct{})
	s.exitChan = make(chan struct{})

	s.logger.Info("executor.watching-for-desired-lrp")

	go func() {
		lrps, stopChan, errChan := s.bbs.WatchForDesiredTransitionalLongRunningProcess()
		if readyChan != nil {
			close(readyChan)
		}
		for {
			select {
			case err := <-errChan:
				s.logger.Errord(map[string]interface{}{
					"error": err.Error(),
				}, "game-scheduler.watch-desired.restart")
				lrps, stopChan, errChan = s.bbs.WatchForDesiredTransitionalLongRunningProcess()

			case lrp, ok := <-lrps:
				if !ok {
					s.logger.Errord(map[string]interface{}{
						"error": errors.New("lrp channel closed. This is very unexpected, we did not intented to exit like this."),
					}, "game-scheduler.watch-desired.lrp-chan-closed")
					close(s.terminatedChan)
					return
				}
				s.inFlight.Add(1)
				go func() {
					s.handleLrpRequest(lrp)
					s.inFlight.Done()
				}()

			case <-s.exitChan:
				s.inFlight.Wait()
				close(stopChan)
				close(s.terminatedChan)
				return
			}
		}
	}()

}

func (s *LrpScheduler) Stop() {
	if s.exitChan != nil {
		close(s.exitChan)
		<-s.terminatedChan
	}
}

func (s *LrpScheduler) handleLrpRequest(lrp models.TransitionalLongRunningProcess) {
	var err error

	if lrp.Stack != s.stack {
		return
	}

	container, err := s.client.AllocateContainer(lrp.Guid, api.ContainerAllocationRequest{
		MemoryMB: lrp.MemoryMB,
		DiskMB:   lrp.DiskMB,
	})

	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "lrp-scheduler.allocation-request.failed")
		return
	}

	s.sleepForARandomInterval()

	err = s.bbs.StartTransitionalLongRunningProcess(lrp)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "lrp-scheduler.start-lrp.failed")
		s.client.DeleteContainer(container.Guid)
		return
	}

	err = s.client.InitializeContainer(container.Guid, api.ContainerInitializationRequest{
		Log: lrp.Log,
	})
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "lrp-scheduler.initialize-container-request.failed")
		s.client.DeleteContainer(container.Guid)
		return
	}

	err = s.client.Run(container.Guid, api.ContainerRunRequest{
		Actions: lrp.Actions,
	})
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "lrp-scheduler.run-actions.failed")
	}
}

func (s *LrpScheduler) sleepForARandomInterval() {
	interval := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(100)
	time.Sleep(time.Duration(interval) * time.Millisecond)
}
