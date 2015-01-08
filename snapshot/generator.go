package snapshot

import (
	"fmt"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/operationq"
)

//go:generate counterfeiter -o fake_snapshot/fake_generator.go . Generator

type Generator interface {
	BatchOperations(lager.Logger) ([]operationq.Operation, error)
	OperationStream(lager.Logger) (<-chan operationq.Operation, error)
}

type generator struct {
	cellID         string
	bbs            bbs.RepBBS
	executorClient executor.Client
	processor      SnapshotProcessor
}

func NewGenerator(cellID string, bbs bbs.RepBBS, executorClient executor.Client, processor SnapshotProcessor) Generator {
	return &generator{
		cellID:         cellID,
		bbs:            bbs,
		executorClient: executorClient,
		processor:      processor,
	}
}

func (g *generator) BatchOperations(logger lager.Logger) ([]operationq.Operation, error) {
	containers := make(map[string]executor.Container)
	lrps := make(map[string]models.ActualLRP)
	tasks := make(map[string]models.Task)

	errChan := make(chan error, 3)

	go func() {
		foundContainers, err := g.executorClient.ListContainers(nil)
		if err != nil {
			err = fmt.Errorf("snapshot-ListContainers failed: %s", err.Error())
		}

		for _, c := range foundContainers {
			containers[c.Guid] = c
		}

		errChan <- err
	}()

	go func() {
		foundLRPS, err := g.bbs.ActualLRPsByCellID(g.cellID)
		if err != nil {
			err = fmt.Errorf("snapshot-ActualLRPsByCellID failed: %s", err.Error())
		}

		for _, lrp := range foundLRPS {
			lrps[lrp.InstanceGuid] = lrp
		}
		errChan <- err
	}()

	go func() {
		foundTasks, err := g.bbs.TasksByCellID(logger, g.cellID)
		if err != nil {
			err = fmt.Errorf("snapshot-TasksByCellID failed: %s", err.Error())
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

	album := make(map[string]operationq.Operation)

	// create snapshots for processes with containers
	for guid, container := range containers {
		snap, err := g.snapshotFromContainerAndTasks(container, tasks)
		if err != nil {
			continue
		}

		album[guid] = NewOperation(logger, snap, g.processor)
	}

	// create snapshots for lrps with no containers
	for guid, lrp := range lrps {
		_, found := album[guid]
		if !found {
			snap := NewLRPSnapshot(NewLRPFromModel(lrp), nil)
			album[guid] = NewOperation(logger, snap, g.processor)
		}
	}

	// create snapshots for tasks with no containers
	for guid, task := range tasks {
		_, found := album[guid]
		if !found {
			snap := NewTaskSnapshot(NewTaskFromModel(task), nil)
			album[guid] = NewOperation(logger, snap, g.processor)
		}
	}

	ops := make([]operationq.Operation, 0, len(album))
	for _, op := range album {
		ops = append(ops, op)
	}

	return ops, nil
}

func (g *generator) OperationStream(logger lager.Logger) (<-chan operationq.Operation, error) {
	events, err := g.executorClient.SubscribeToEvents()
	if err != nil {
		return nil, err
	}

	opChan := make(chan operationq.Operation)

	go func() {
		for {
			select {
			case e, ok := <-events:
				if !ok {
					close(opChan)
					return
				}

				lifecycle, ok := e.(executor.LifecycleEvent)
				if !ok {
					continue
				}

				container := lifecycle.Container()
				session := logger.Session("operation-from-container", lager.Data{"container-guid": container.Guid})
				session.Debug("started")
				operation, err := g.operationFromContainer(session, container)
				if err != nil {
					session.Error("failed", err)
					continue
				}
				session.Debug("completed")
				opChan <- operation
			}
		}
	}()

	return opChan, nil
}

func (g *generator) operationFromContainer(logger lager.Logger, container executor.Container) (operationq.Operation, error) {
	lifecycle := container.Tags[rep.LifecycleTag]

	switch lifecycle {
	case rep.LRPLifecycle:
		lrpKey, err := rep.ActualLRPKeyFromContainer(container)
		if err != nil {
			return nil, err
		}

		containerKey, err := rep.ActualLRPContainerKeyFromContainer(container, g.cellID)
		if err != nil {
			return nil, err
		}

		lrp := NewLRP(lrpKey, containerKey)
		return NewOperation(logger, NewLRPSnapshot(lrp, &container), g.processor), nil

	case rep.TaskLifecycle:
		task, err := g.bbs.TaskByGuid(container.Guid)
		if err != nil {
			return nil, err
		}

		taskSnap := NewTask(task.TaskGuid, g.cellID, task.State, task.ResultFile)
		return NewOperation(logger, NewTaskSnapshot(taskSnap, &container), g.processor), nil

	default:
		return nil, fmt.Errorf("unknown lifecycle: %s", lifecycle)
	}
}

func (g *generator) snapshotFromContainerAndTasks(container executor.Container, tasks map[string]models.Task) (Snapshot, error) {
	lifecycle := container.Tags[rep.LifecycleTag]

	switch lifecycle {
	case rep.LRPLifecycle:
		lrpKey, err := rep.ActualLRPKeyFromContainer(container)
		if err != nil {
			return nil, err
		}

		containerKey, err := rep.ActualLRPContainerKeyFromContainer(container, g.cellID)
		if err != nil {
			return nil, err
		}

		lrp := NewLRP(lrpKey, containerKey)
		return NewLRPSnapshot(lrp, &container), nil

	case rep.TaskLifecycle:
		task, found := tasks[container.Guid]
		if !found {
			return NewTaskSnapshot(nil, &container), nil
		}

		return NewTaskSnapshot(NewTaskFromModel(task), &container), nil

	default:
		return nil, fmt.Errorf("unknown lifecycle: %s", lifecycle)
	}
}
