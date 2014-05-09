package lrp_scheduler

import (
	"errors"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/cloudfoundry-incubator/executor/client"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gosteno"
)

type LrpScheduler struct {
	bbs      bbs.ExecutorBBS
	logger   *gosteno.Logger
	client   client.Client
	inFlight *sync.WaitGroup
}

func New(bbs bbs.ExecutorBBS, logger *gosteno.Logger, executorClient client.Client) *LrpScheduler {
	return &LrpScheduler{
		bbs:      bbs,
		logger:   logger,
		client:   executorClient,
		inFlight: &sync.WaitGroup{},
	}
}

func (s *LrpScheduler) Run(sigChan chan os.Signal, readyChan chan struct{}) error {
	s.logger.Info("executor.watching-for-desired-lrp")
	lrps, _, errChan := s.bbs.WatchForDesiredTransitionalLongRunningProcess()

	if readyChan != nil {
		close(readyChan)
	}

	for {
		select {
		case err := <-errChan:
			s.logger.Errord(map[string]interface{}{
				"error": err.Error(),
			}, "game-scheduler.watch-desired.restart")
			lrps, _, errChan = s.bbs.WatchForDesiredTransitionalLongRunningProcess()

		case lrp, ok := <-lrps:
			if !ok {
				s.logger.Errord(map[string]interface{}{
					"error": errors.New("lrp channel closed. This is very unexpected, we did not intented to exit like this."),
				}, "game-scheduler.watch-desired.lrp-chan-closed")
				return nil
			}
			s.inFlight.Add(1)
			go func() {
				s.handleLrpRequest(lrp)
				s.inFlight.Done()
			}()

		case sig := <-sigChan:
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				s.inFlight.Wait()

				// TODO
				// close(stopChan)

				return nil
			}
		}
	}
}

func (s *LrpScheduler) handleLrpRequest(lrp models.TransitionalLongRunningProcess) {
	var err error

	container, err := s.client.AllocateContainer(client.ContainerRequest{

		// TODO
		// DiskMB:     lrp.DiskMB,
		// MemoryMB:   lrp.MemoryMB,
		// CpuPercent: lrp.CpuPercent,

		LogConfig: lrp.Log,
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

	err = s.client.InitializeContainer(container.Guid)
	if err != nil {
		s.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "lrp-scheduler.initialize-container-request.failed")
		s.client.DeleteContainer(container.Guid)
		return
	}

	err = s.client.Run(container.Guid, client.RunRequest{
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
