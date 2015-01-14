package generator

import (
	"fmt"

	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/generator/internal"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

// MissingLRPOperation processes an ActualLRP with no matching container.
type MissingLRPOperation struct {
	logger            lager.Logger
	bbs               bbs.RepBBS
	containerDelegate internal.ContainerDelegate
	models.ActualLRPKey
	models.ActualLRPContainerKey
}

func NewMissingLRPOperation(logger lager.Logger,
	bbs bbs.RepBBS,
	containerDelegate internal.ContainerDelegate,
	lrpKey models.ActualLRPKey,
	containerKey models.ActualLRPContainerKey,
) *MissingLRPOperation {
	return &MissingLRPOperation{
		logger:                logger,
		bbs:                   bbs,
		containerDelegate:     containerDelegate,
		ActualLRPKey:          lrpKey,
		ActualLRPContainerKey: containerKey,
	}
}

func (o *MissingLRPOperation) Key() string {
	return o.InstanceGuid
}

func (o *MissingLRPOperation) Execute() {
	logger := o.logger.Session("executing-missing-lrp-operation", lager.Data{
		"lrp-key":       o.ActualLRPKey,
		"container-key": o.ActualLRPContainerKey,
	})
	logger.Info("starting")
	defer logger.Info("finished")

	_, exists := o.containerDelegate.GetContainer(logger, o.InstanceGuid)
	if exists {
		logger.Info("skipped-because-container-exists")
		return
	}

	o.bbs.RemoveActualLRP(o.ActualLRPKey, o.ActualLRPContainerKey, logger)
}

// MissingTaskOperation processes a Task with no matching container.
type MissingTaskOperation struct {
	logger            lager.Logger
	bbs               bbs.RepBBS
	containerDelegate internal.ContainerDelegate
	TaskGuid          string
}

func NewMissingTaskOperation(logger lager.Logger, bbs bbs.RepBBS, containerDelegate internal.ContainerDelegate, taskGuid string) *MissingTaskOperation {
	return &MissingTaskOperation{
		logger:            logger,
		bbs:               bbs,
		TaskGuid:          taskGuid,
		containerDelegate: containerDelegate,
	}
}
func (o *MissingTaskOperation) Key() string {
	return o.TaskGuid
}

func (o *MissingTaskOperation) Execute() {
	logger := o.logger.Session("executing-missing-task-operation", lager.Data{
		"task-guid": o.TaskGuid,
	})
	logger.Info("starting")
	defer logger.Info("finished")

	_, exists := o.containerDelegate.GetContainer(logger, o.TaskGuid)
	if exists {
		logger.Info("skipped-because-container-exists")
		return
	}

	task, err := o.bbs.TaskByGuid(o.TaskGuid)
	if err != nil {
		logger.Error("failed-to-fetch-task", err)
		return
	}

	if task.State != models.TaskStateRunning {
		logger.Info("skipped-because-task-is-not-running")
		return
	}

	err = o.bbs.FailTask(logger, o.TaskGuid, internal.TaskCompletionReasonMissingContainer)
	if err != nil {
		logger.Error("failed-to-fail-task", err)
	}
}

// ContainerOperation acquires the current state of a container and performs any
// bbs or container operations necessary to harmonize the state of the world.
type ContainerOperation struct {
	logger            lager.Logger
	lrpProcessor      internal.LRPProcessor
	taskProcessor     internal.TaskProcessor
	containerDelegate internal.ContainerDelegate
	Guid              string
}

func NewContainerOperation(
	logger lager.Logger,
	lrpProcessor internal.LRPProcessor,
	taskProcessor internal.TaskProcessor,
	containerDelegate internal.ContainerDelegate,
	guid string,
) *ContainerOperation {
	return &ContainerOperation{
		logger:            logger,
		lrpProcessor:      lrpProcessor,
		taskProcessor:     taskProcessor,
		containerDelegate: containerDelegate,
		Guid:              guid,
	}
}

func (o *ContainerOperation) Key() string {
	return o.Guid
}

func (o *ContainerOperation) Execute() {
	logger := o.logger.Session("executing-container-operation", lager.Data{
		"container-guid": o.Guid,
	})
	logger.Info("starting")
	defer logger.Info("finished")

	container, ok := o.containerDelegate.GetContainer(logger, o.Guid)
	if !ok {
		logger.Info("skipped-because-container-does-not-exist")
		return
	}

	logger = logger.WithData(lager.Data{
		"container-state": container.State,
	})
	lifecycle := container.Tags[rep.LifecycleTag]

	switch lifecycle {
	case rep.LRPLifecycle:
		o.lrpProcessor.Process(logger, container)
		return

	case rep.TaskLifecycle:
		o.taskProcessor.Process(logger, container)
		return

	default:
		logger.Error("failed-to-process-container-with-unknown-lifecycle", fmt.Errorf("unknown lifecycle: %s", lifecycle))
		return
	}
}
