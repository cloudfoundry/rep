package evacuation

import (
	"os"
	"syscall"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context"
	"github.com/cloudfoundry-incubator/rep/generator"
	"github.com/cloudfoundry-incubator/rep/generator/internal"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/pivotal-golang/clock"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/operationq"
)

type Evacuator struct {
	logger            lager.Logger
	executorClient    executor.Client
	bbs               bbs.RepBBS
	evacuatable       evacuation_context.Evacuatable
	lrpProcessor      internal.LRPProcessor
	taskProcessor     internal.TaskProcessor
	containerDelegate internal.ContainerDelegate
	queue             operationq.Queue
	cellID            string
	evacuationTimeout time.Duration
	pollingInterval   time.Duration
	clock             clock.Clock
}

func NewEvacuator(
	logger lager.Logger,
	executorClient executor.Client,
	bbs bbs.RepBBS,
	evacuatable evacuation_context.Evacuatable,
	lrpProcessor internal.LRPProcessor,
	taskProcessor internal.TaskProcessor,
	containerDelegate internal.ContainerDelegate,
	queue operationq.Queue,
	cellID string,
	evacuationTimeout time.Duration,
	pollingInterval time.Duration,
	clock clock.Clock,
) *Evacuator {
	return &Evacuator{
		logger:            logger,
		executorClient:    executorClient,
		bbs:               bbs,
		evacuatable:       evacuatable,
		lrpProcessor:      lrpProcessor,
		taskProcessor:     taskProcessor,
		containerDelegate: containerDelegate,
		queue:             queue,
		cellID:            cellID,
		evacuationTimeout: evacuationTimeout,
		pollingInterval:   pollingInterval,
		clock:             clock,
	}
}

func (e *Evacuator) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	logger := e.logger.Session("run")
	close(ready)

	select {
	case signal := <-signals:
		logger.Info("run-signaled", lager.Data{"signal": signal.String()})
		if signal == syscall.SIGUSR1 {
			e.evacuatable.Evacuate()

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

	done := true

	for _, container := range containers {
		switch container.Tags[rep.LifecycleTag] {
		case rep.TaskLifecycle:
			if container.State != executor.StateCompleted {
				done = false
			}
		case rep.LRPLifecycle:
			done = false
			containerOperation := generator.NewContainerOperation(logger, e.lrpProcessor, e.taskProcessor, e.containerDelegate, container.Guid)
			e.queue.Push(containerOperation)
		}
	}

	return done
}
