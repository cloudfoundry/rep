package evacuation

import (
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context"
	"github.com/pivotal-golang/clock"
	"github.com/pivotal-golang/lager"
)

type Evacuator struct {
	logger             lager.Logger
	clock              clock.Clock
	executorClient     executor.Client
	evacuationNotifier evacuation_context.EvacuationNotifier
	cellID             string
	evacuationTimeout  time.Duration
	pollingInterval    time.Duration
}

func NewEvacuator(
	logger lager.Logger,
	clock clock.Clock,
	executorClient executor.Client,
	evacuationNotifier evacuation_context.EvacuationNotifier,
	cellID string,
	evacuationTimeout time.Duration,
	pollingInterval time.Duration,
) *Evacuator {
	return &Evacuator{
		logger:             logger,
		clock:              clock,
		executorClient:     executorClient,
		evacuationNotifier: evacuationNotifier,
		cellID:             cellID,
		evacuationTimeout:  evacuationTimeout,
		pollingInterval:    pollingInterval,
	}
}

func (e *Evacuator) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	logger := e.logger.Session("running-evacuator")
	evacuationNotify := e.evacuationNotifier.EvacuateNotify()
	close(ready)

	select {
	case <-signals:
		return nil
	case <-evacuationNotify:
		evacuationNotify = nil
	}

	doneCh := make(chan struct{})
	go e.evacuateWithTimeout(logger, doneCh)

	select {
	case <-doneCh:
		logger.Info("evacuation-complete")
		return nil
	case signal := <-signals:
		logger.Info("run-signaled", lager.Data{"signal": signal.String()})
		return nil
	}

	return nil
}

func (e *Evacuator) evacuateWithTimeout(logger lager.Logger, doneCh chan<- struct{}) {
	logger = logger.Session("evacuating-with-timeout")
	logger.Info("started")
	innerDone := make(chan struct{})
	go e.evacuate(innerDone)

	timer := e.clock.NewTimer(e.evacuationTimeout)
	select {
	case <-timer.C():
		logger.Error("failed-to-evacuate-before-timeout", nil)
	case <-innerDone:
		logger.Info("succeeded")
	}
	close(doneCh)
}

func (e *Evacuator) evacuate(doneCh chan<- struct{}) {
	logger := e.logger.Session("evacuate")
	timer := e.clock.NewTimer(e.pollingInterval)

	for {
		evacuated := e.allContainersEvacuated(logger)

		if !evacuated {
			logger.Info("evacuation-incomplete", lager.Data{"polling-interval": e.pollingInterval})
			timer.Reset(e.pollingInterval)
			<-timer.C()
			continue
		}

		close(doneCh)
		return
	}
}

func (e *Evacuator) allContainersEvacuated(logger lager.Logger) bool {
	containers, err := e.executorClient.ListContainers(nil)
	if err != nil {
		logger.Error("failed-to-list-containers", err)
		return false
	}

	return len(containers) == 0
}
