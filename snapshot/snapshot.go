package snapshot

import (
	"errors"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/operationq"
)

var (
	ErrEmptyLRPSnapshot      = errors.New("cannot process empty LRPSnapshot")
	ErrLRPSnapshotMissingLRP = errors.New("cannot process LRPSnapshot with nil LRP")

	ErrEmptyTaskSnapshot = errors.New("cannot process empty TaskSnapshot")
)

//go:generate counterfeiter -o fake_snapshot/fake_snapshot.go . Snapshot

type Snapshot interface {
	Guid() string
}

type snapshotOperation struct {
	logger    lager.Logger
	snapshot  Snapshot
	processor SnapshotProcessor
}

func (o *snapshotOperation) Key() string {
	return o.snapshot.Guid()
}

func (o *snapshotOperation) Execute() {
	o.processor.Process(o.logger, o.snapshot)
}

func NewOperation(logger lager.Logger, snap Snapshot, processor SnapshotProcessor) operationq.Operation {
	return &snapshotOperation{
		logger:    logger,
		snapshot:  snap,
		processor: processor,
	}
}

type Album map[string]operationq.Operation

type LRP struct {
	models.ActualLRPKey
	models.ActualLRPContainerKey
}

func NewLRP(lrpKey models.ActualLRPKey, containerKey models.ActualLRPContainerKey) *LRP {
	return &LRP{
		ActualLRPKey:          lrpKey,
		ActualLRPContainerKey: containerKey,
	}
}

func NewLRPFromModel(lrp models.ActualLRP) *LRP {
	return NewLRP(lrp.ActualLRPKey, lrp.ActualLRPContainerKey)
}

type LRPSnapshot struct {
	*LRP
	*executor.Container
}

func NewLRPSnapshot(lrp *LRP, container *executor.Container) *LRPSnapshot {
	return &LRPSnapshot{
		LRP:       lrp,
		Container: container,
	}
}

func (snap LRPSnapshot) Guid() string {
	if snap.LRP != nil {
		return snap.LRP.ProcessGuid
	}
	return ""
}

func (snap LRPSnapshot) Validate() error {
	if snap.LRP == nil {
		if snap.Container == nil {
			return ErrEmptyLRPSnapshot
		} else {
			return ErrLRPSnapshotMissingLRP
		}
	}

	return nil
}

type Task struct {
	TaskGuid   string
	CellID     string
	State      models.TaskState
	ResultFile string
}

func NewTask(taskGuid string, cellID string, state models.TaskState, resultFile string) *Task {
	return &Task{
		TaskGuid:   taskGuid,
		CellID:     cellID,
		State:      state,
		ResultFile: resultFile,
	}
}

func NewTaskFromModel(task models.Task) *Task {
	return NewTask(task.TaskGuid, task.CellID, task.State, task.ResultFile)
}

type TaskSnapshot struct {
	Task      *Task
	Container *executor.Container
}

func NewTaskSnapshot(task *Task, container *executor.Container) *TaskSnapshot {
	return &TaskSnapshot{
		Task:      task,
		Container: container,
	}
}

func (snap TaskSnapshot) Guid() string {
	if snap.Container != nil {
		return snap.Container.Guid
	}
	if snap.Task != nil {
		return snap.Task.TaskGuid
	}
	return ""
}

func (snap TaskSnapshot) Validate() error {
	if snap.Task == nil && snap.Container == nil {
		return ErrEmptyTaskSnapshot
	}

	return nil
}
