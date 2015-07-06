// generator creates operations for container processing
package generator

import (
	"fmt"

	"github.com/cloudfoundry-incubator/bbs"
	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/generator/internal"
	legacybbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	oldmodels "github.com/cloudfoundry-incubator/runtime-schema/models"
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
	legacyBBS         legacybbs.RepBBS
	bbs               bbs.Client
	executorClient    executor.Client
	lrpProcessor      internal.LRPProcessor
	taskProcessor     internal.TaskProcessor
	containerDelegate internal.ContainerDelegate
}

func New(
	cellID string,
	bbs bbs.Client,
	legacyBBS legacybbs.RepBBS,
	executorClient executor.Client,
	lrpProcessor internal.LRPProcessor,
	taskProcessor internal.TaskProcessor,
	containerDelegate internal.ContainerDelegate,
) Generator {
	return &generator{
		cellID:            cellID,
		bbs:               bbs,
		legacyBBS:         legacyBBS,
		executorClient:    executorClient,
		lrpProcessor:      lrpProcessor,
		taskProcessor:     taskProcessor,
		containerDelegate: containerDelegate,
	}
}

func (g *generator) BatchOperations(logger lager.Logger) (map[string]operationq.Operation, error) {
	logger = logger.Session("batch-operations")
	logger.Info("started")

	containers := make(map[string]executor.Container)
	instanceLRPs := make(map[string]models.ActualLRP)
	evacuatingLRPs := make(map[string]models.ActualLRP)
	tasks := make(map[string]oldmodels.Task)

	errChan := make(chan error, 3)

	logger.Info("getting-containers-lrps-and-tasks")
	go func() {
		foundContainers, err := g.executorClient.ListContainers(nil)
		if err != nil {
			logger.Error("failed-to-list-containers", err)
			err = fmt.Errorf("failed to list containers: %s", err.Error())
		}

		for _, c := range foundContainers {
			containers[c.Guid] = c
		}

		errChan <- err
	}()

	go func() {
		filter := models.ActualLRPFilter{CellID: g.cellID}
		groups, err := g.bbs.ActualLRPGroups(filter)
		if err != nil {
			logger.Error("failed-to-retrieve-lrp-groups", err)
			err = fmt.Errorf("failed to retrieve lrps: %s", err.Error())
		}

		for _, group := range groups {
			if group.Instance != nil {
				instanceLRPs[rep.LRPContainerGuid(group.Instance.GetProcessGuid(), group.Instance.GetInstanceGuid())] = *group.Instance
			}
			if group.Evacuating != nil {
				evacuatingLRPs[rep.LRPContainerGuid(group.Evacuating.GetProcessGuid(), group.Evacuating.GetInstanceGuid())] = *group.Evacuating
			}
		}
		errChan <- err
	}()

	go func() {
		foundTasks, err := g.legacyBBS.TasksByCellID(logger, g.cellID)
		if err != nil {
			logger.Error("failed-to-retrieve-tasks", err)
			err = fmt.Errorf("failed to retrieve tasks: %s", err.Error())
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
		logger.Error("failed-getting-containers-lrps-and-tasks", err)
		return nil, err
	}
	logger.Info("succeeded-getting-containers-lrps-and-tasks")

	batch := make(map[string]operationq.Operation)

	// create operations for processes with containers
	for guid, _ := range containers {
		batch[guid] = g.operationFromContainer(logger, guid)
	}

	// create operations for instance lrps with no containers
	for guid, lrp := range instanceLRPs {
		if _, foundContainer := batch[guid]; foundContainer {
			continue
		}
		if _, foundEvacuatingLRP := evacuatingLRPs[guid]; foundEvacuatingLRP {
			batch[guid] = NewResidualJointLRPOperation(logger, g.legacyBBS, g.containerDelegate, lrp.ActualLRPKey, lrp.ActualLRPInstanceKey)
		} else {
			batch[guid] = NewResidualInstanceLRPOperation(logger, g.legacyBBS, g.containerDelegate, lrp.ActualLRPKey, lrp.ActualLRPInstanceKey)
		}
	}

	// create operations for evacuating lrps with no containers
	for guid, lrp := range evacuatingLRPs {
		_, found := batch[guid]
		if !found {
			batch[guid] = NewResidualEvacuatingLRPOperation(logger, g.legacyBBS, g.containerDelegate, lrp.ActualLRPKey, lrp.ActualLRPInstanceKey)
		}
	}

	// create operations for tasks with no containers
	for guid, _ := range tasks {
		_, found := batch[guid]
		if !found {
			batch[guid] = NewResidualTaskOperation(logger, g.legacyBBS, g.containerDelegate, guid)
		}
	}

	logger.Info("succeeded", lager.Data{"batch-size": len(batch)})
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
		defer events.Close()

		for {
			e, err := events.Next()
			if err != nil {
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
	}()

	return opChan, nil
}

func (g *generator) operationFromContainer(logger lager.Logger, guid string) operationq.Operation {
	return NewContainerOperation(logger, g.lrpProcessor, g.taskProcessor, g.containerDelegate, guid)
}
