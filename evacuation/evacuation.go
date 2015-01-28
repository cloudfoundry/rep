package evacuation

import (
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/pivotal-golang/clock"
	"github.com/pivotal-golang/lager"
)

//go:generate counterfeiter -o fake_evacuator/fake_evacuator.go . EvacuationContext
type EvacuationContext interface {
	Evacuating() bool
}

type evacuationContext struct {
	evacuating int32
}

func (e *evacuationContext) Evacuating() bool {
	return atomic.LoadInt32(&e.evacuating) != 0
}

type Evacuator struct {
	logger            lager.Logger
	executorClient    executor.Client
	bbs               bbs.RepBBS
	cellID            string
	evacuationTimeout time.Duration
	pollingInterval   time.Duration
	clock             clock.Clock

	evacuationContext evacuationContext
}

func NewEvacuator(
	logger lager.Logger,
	executorClient executor.Client,
	bbs bbs.RepBBS,
	cellID string,
	evacuationTimeout time.Duration,
	pollingInterval time.Duration,
	clock clock.Clock,
) *Evacuator {
	return &Evacuator{
		logger:            logger,
		executorClient:    executorClient,
		bbs:               bbs,
		cellID:            cellID,
		evacuationTimeout: evacuationTimeout,
		pollingInterval:   pollingInterval,
		clock:             clock,

		evacuationContext: evacuationContext{},
	}
}

func (e *Evacuator) EvacuationContext() EvacuationContext {
	return &e.evacuationContext
}

func (e *Evacuator) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	logger := e.logger.Session("run")
	close(ready)

	select {
	case signal := <-signals:
		logger.Info("run-signaled", lager.Data{"signal": signal.String()})
		if signal == syscall.SIGUSR1 {
			atomic.AddInt32(&e.evacuationContext.evacuating, 1)

			doneCh := make(chan struct{})
			go e.evacuate(doneCh)

			timer := e.clock.NewTimer(e.evacuationTimeout)
			select {
			case <-timer.C():
			case <-doneCh:
			}
		}
	}

	return nil
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

	for _, container := range containers {
		switch container.Tags[rep.LifecycleTag] {
		case rep.TaskLifecycle:
			if container.State != executor.StateCompleted {
				return false
			}
		case rep.LRPLifecycle:
			err := e.evacuateLRP(logger, container)
			if err != nil {
				logger.Error("evacuation-failed", err)
			}
		}
	}

	return true
}

func (e *Evacuator) evacuateLRP(logger lager.Logger, container executor.Container) error {
	lrpKey, err := rep.ActualLRPKeyFromContainer(container)
	if err != nil {
		return err
	}

	if container.State == executor.StateReserved {
		containerKey, err := rep.ActualLRPContainerKeyFromContainer(container, e.cellID)
		if err != nil {
			return err
		}

		err = e.bbs.EvacuateActualLRP(logger, lrpKey, containerKey)
		if err != nil {
			return err
		}

		err = e.executorClient.StopContainer(container.Guid)
		if err != nil {
			return err
		}
	}

	return nil
}
