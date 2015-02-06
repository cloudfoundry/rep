package generator

import (
	"fmt"

	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/generator/internal"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

// ResidualInstanceLRPOperation processes an instance ActualLRP with no matching container.
type ResidualInstanceLRPOperation struct {
	logger            lager.Logger
	bbs               bbs.RepBBS
	containerDelegate internal.ContainerDelegate
	models.ActualLRPKey
	models.ActualLRPContainerKey
}

func NewResidualInstanceLRPOperation(logger lager.Logger,
	bbs bbs.RepBBS,
	containerDelegate internal.ContainerDelegate,
	lrpKey models.ActualLRPKey,
	containerKey models.ActualLRPContainerKey,
) *ResidualInstanceLRPOperation {
	return &ResidualInstanceLRPOperation{
		logger:                logger,
		bbs:                   bbs,
		containerDelegate:     containerDelegate,
		ActualLRPKey:          lrpKey,
		ActualLRPContainerKey: containerKey,
	}
}

func (o *ResidualInstanceLRPOperation) Key() string {
	return o.InstanceGuid
}

func (o *ResidualInstanceLRPOperation) Execute() {
	logger := o.logger.Session("executing-residual-instance-lrp-operation", lager.Data{
		"lrp-key":       o.ActualLRPKey,
		"lrp-container-key": o.ActualLRPContainerKey,
	})
	logger.Info("starting")
	defer logger.Info("finished")

	_, exists := o.containerDelegate.GetContainer(logger, o.InstanceGuid)
	if exists {
		logger.Info("skipped-because-container-exists")
		return
	}

	o.bbs.RemoveActualLRP(logger, o.ActualLRPKey, o.ActualLRPContainerKey)
}

// ResidualEvacuatingLRPOperation processes an evacuating ActualLRP with no matching container.
type ResidualEvacuatingLRPOperation struct {
	logger            lager.Logger
	bbs               bbs.RepBBS
	containerDelegate internal.ContainerDelegate
	models.ActualLRPKey
	models.ActualLRPContainerKey
}

func NewResidualEvacuatingLRPOperation(logger lager.Logger,
	bbs bbs.RepBBS,
	containerDelegate internal.ContainerDelegate,
	lrpKey models.ActualLRPKey,
	containerKey models.ActualLRPContainerKey,
) *ResidualEvacuatingLRPOperation {
	return &ResidualEvacuatingLRPOperation{
		logger:                logger,
		bbs:                   bbs,
		containerDelegate:     containerDelegate,
		ActualLRPKey:          lrpKey,
		ActualLRPContainerKey: containerKey,
	}
}

func (o *ResidualEvacuatingLRPOperation) Key() string {
	return o.InstanceGuid
}

func (o *ResidualEvacuatingLRPOperation) Execute() {
	logger := o.logger.Session("executing-residual-evacuating-lrp-operation", lager.Data{
		"lrp-key":       o.ActualLRPKey,
		"lrp-container-key": o.ActualLRPContainerKey,
	})
	logger.Info("starting")
	defer logger.Info("finished")

	_, exists := o.containerDelegate.GetContainer(logger, o.InstanceGuid)
	if exists {
		logger.Info("skipped-because-container-exists")
		return
	}

	o.bbs.RemoveEvacuatingActualLRP(logger, o.ActualLRPKey, o.ActualLRPContainerKey)
}

// ResidualTaskOperation processes a Task with no matching container.
type ResidualTaskOperation struct {
	logger            lager.Logger
	bbs               bbs.RepBBS
	containerDelegate internal.ContainerDelegate
	TaskGuid          string
}

func NewResidualTaskOperation(
	logger lager.Logger,
	bbs bbs.RepBBS,
	containerDelegate internal.ContainerDelegate,
	taskGuid string,
) *ResidualTaskOperation {
	return &ResidualTaskOperation{
		logger:            logger,
		bbs:               bbs,
		TaskGuid:          taskGuid,
		containerDelegate: containerDelegate,
	}
}

func (o *ResidualTaskOperation) Key() string {
	return o.TaskGuid
}

func (o *ResidualTaskOperation) Execute() {
	logger := o.logger.Session("executing-residual-task-operation", lager.Data{
		"task-guid": o.TaskGuid,
	})
	logger.Info("starting")
	defer logger.Info("finished")

	_, exists := o.containerDelegate.GetContainer(logger, o.TaskGuid)
	if exists {
		logger.Info("skipped-because-container-exists")
		return
	}

	err := o.bbs.FailTask(logger, o.TaskGuid, internal.TaskCompletionReasonMissingContainer)
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
