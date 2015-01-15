// generator creates operations for container processing
package generator

import (
	"fmt"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep/generator/internal"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/operationq"
)

//go:generate counterfeiter -o fake_generator/fake_generator.go . Generator

// Generator encapsulates operation creation in the Rep.
type Generator interface {
	// BatchOperations creates a set of operations across all containers the Rep is managing.
	BatchOperations(lager.Logger) (map[string]operationq.Operation, error)

	// OperationStream creates an operation every time a container lifecycle event is observed.
	OperationStream(lager.Logger) (<-chan operationq.Operation, error)
}

type generator struct {
	cellID            string
	bbs               bbs.RepBBS
	executorClient    executor.Client
	lrpProcessor      internal.LRPProcessor
	taskProcessor     internal.TaskProcessor
	containerDelegate internal.ContainerDelegate
}

func New(cellID string, bbs bbs.RepBBS, executorClient executor.Client) Generator {
	containerDelegate := internal.NewContainerDelegate(executorClient)
	lrpProcessor := internal.NewLRPProcessor(bbs, containerDelegate, cellID)
	taskProcessor := internal.NewTaskProcessor(bbs, containerDelegate, cellID)

	return &generator{
		cellID:            cellID,
		bbs:               bbs,
		executorClient:    executorClient,
		lrpProcessor:      lrpProcessor,
		taskProcessor:     taskProcessor,
		containerDelegate: containerDelegate,
	}
}

func (g *generator) BatchOperations(logger lager.Logger) (map[string]operationq.Operation, error) {
	containers := make(map[string]executor.Container)
	lrps := make(map[string]models.ActualLRP)
	tasks := make(map[string]models.Task)

	errChan := make(chan error, 3)

	go func() {
		foundContainers, err := g.executorClient.ListContainers(nil)
		if err != nil {
			err = fmt.Errorf("generator-ListContainers failed: %s", err.Error())
		}

		for _, c := range foundContainers {
			containers[c.Guid] = c
		}

		errChan <- err
	}()

	go func() {
		foundLRPS, err := g.bbs.ActualLRPsByCellID(g.cellID)
		if err != nil {
			err = fmt.Errorf("generator-ActualLRPsByCellID failed: %s", err.Error())
		}

		for _, lrp := range foundLRPS {
			lrps[lrp.InstanceGuid] = lrp
		}
		errChan <- err
	}()

	go func() {
		foundTasks, err := g.bbs.TasksByCellID(logger, g.cellID)
		if err != nil {
			err = fmt.Errorf("generator-TasksByCellID failed: %s", err.Error())
		}

		for _, task := range foundTasks {
			tasks[task.TaskGuid] = task
		}
		errChan <- err
	}()

	var err error
	for i := 0; i < 3; i++ {
		e := <-errChan
		if err == nil && e != nil {
			err = e
		}
	}

	if err != nil {
		return nil, err
	}

	batch := make(map[string]operationq.Operation)

	// create operations for processes with containers
	for guid, _ := range containers {
		batch[guid] = g.operationFromContainer(logger, guid)
	}

	// create operations for lrps with no containers
	for guid, lrp := range lrps {
		_, found := batch[guid]
		if !found {
			batch[guid] = NewMissingLRPOperation(logger, g.bbs, g.containerDelegate, lrp.ActualLRPKey, lrp.ActualLRPContainerKey)
		}
	}

	// create operations for tasks with no containers
	for guid, _ := range tasks {
		_, found := batch[guid]
		if !found {
			batch[guid] = NewMissingTaskOperation(logger, g.bbs, g.containerDelegate, guid)
		}
	}

	return batch, nil
}

func (g *generator) OperationStream(logger lager.Logger) (<-chan operationq.Operation, error) {
	logger = logger.Session("operation-stream")

	logger.Info("subscribing")
	events, err := g.executorClient.SubscribeToEvents()
	if err != nil {
		logger.Error("failed-subscribing", err)
		return nil, err
	}

	logger.Info("succeeded-subscribing")

	opChan := make(chan operationq.Operation)

	go func() {
		for {
			select {
			case e, ok := <-events:
				if !ok {
					logger.Debug("event-stream-closed")
					close(opChan)
					return
				}

				lifecycle, ok := e.(executor.LifecycleEvent)
				if !ok {
					logger.Debug("received-non-lifecycle-event")
					continue
				}

				container := lifecycle.Container()
				opChan <- g.operationFromContainer(logger, container.Guid)
			}
		}
	}()

	return opChan, nil
}

func (g *generator) operationFromContainer(logger lager.Logger, guid string) operationq.Operation {
	return NewContainerOperation(logger, g.lrpProcessor, g.taskProcessor, g.containerDelegate, guid)
}
