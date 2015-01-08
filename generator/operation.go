package generator

import (
	"fmt"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/generator/internal"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

// MissingLRPOperation processes an ActualLRP with no matching container.
type MissingLRPOperation struct {
	logger lager.Logger
	bbs    bbs.RepBBS
	models.ActualLRPKey
	models.ActualLRPContainerKey
}

func newMissingLRPOperation(logger lager.Logger, bbs bbs.RepBBS, lrpKey models.ActualLRPKey, containerKey models.ActualLRPContainerKey) *MissingLRPOperation {
	return &MissingLRPOperation{
		logger:                logger,
		bbs:                   bbs,
		ActualLRPKey:          lrpKey,
		ActualLRPContainerKey: containerKey,
	}
}

func (o *MissingLRPOperation) Key() string {
	return o.InstanceGuid
}

func (o *MissingLRPOperation) Execute() {
	o.bbs.RemoveActualLRP(o.ActualLRPKey, o.ActualLRPContainerKey, o.logger)
}

// MissingTaskOperation processes a Task with no matching container.
type MissingTaskOperation struct {
	logger   lager.Logger
	bbs      bbs.RepBBS
	TaskGuid string
}

func newMissingTaskOperation(logger lager.Logger, bbs bbs.RepBBS, taskGuid string) *MissingTaskOperation {
	return &MissingTaskOperation{
		logger:   logger,
		bbs:      bbs,
		TaskGuid: taskGuid,
	}
}
func (o *MissingTaskOperation) Key() string {
	return o.TaskGuid
}

func (o *MissingTaskOperation) Execute() {
	o.logger.Debug("start")
	defer o.logger.Debug("complete")

	task, err := o.bbs.TaskByGuid(o.TaskGuid)
	if err != nil {
		o.logger.Error("failed-fetch-task", err)
		return
	}

	if task.State == models.TaskStateRunning {
		err := o.bbs.FailTask(o.logger, o.TaskGuid, internal.TaskCompletionReasonMissingContainer)
		if err != nil {
			o.logger.Error("failed-fail-task", err)
			return
		}
	}
}

// ContainerOperation acquires the current state of a container and performs any
// bbs or container operations necessary to harmonize the state of the world.
type ContainerOperation struct {
	logger            lager.Logger
	bbs               bbs.RepBBS
	lrpProcessor      internal.LRPProcessor
	taskProcessor     internal.TaskProcessor
	containerDelegate internal.ContainerDelegate
	Guid              string
}

func newContainerOperation(
	logger lager.Logger,
	bbs bbs.RepBBS,
	lrpProcessor internal.LRPProcessor,
	taskProcessor internal.TaskProcessor,
	containerDelegate internal.ContainerDelegate,
	guid string,
) *ContainerOperation {
	return &ContainerOperation{
		logger:            logger.Session("container-op"),
		bbs:               bbs,
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
	container, ok := o.containerDelegate.GetContainer(o.logger, o.Guid)
	if !ok {
		return
	}

	o.logger = o.logger.WithData(lagerDataFromContainer(container))
	lifecycle := container.Tags[rep.LifecycleTag]

	switch lifecycle {
	case rep.LRPLifecycle:
		o.lrpProcessor.Process(o.logger, container)
		return

	case rep.TaskLifecycle:
		o.taskProcessor.Process(o.logger, container)
		return

	default:
		o.logger.Error("unknown-lifecycle", fmt.Errorf("unknown lifecycle: %s", lifecycle))
		return
	}
}

func lagerDataFromContainer(container executor.Container) lager.Data {
	data := lager.Data{}
	data["container-guid"] = container.Guid
	data["container-state"] = container.State

	return data
}
