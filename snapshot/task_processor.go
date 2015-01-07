package snapshot

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/bbserrors"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

const TaskCompletionReasonMissingContainer = "task container does not exist"
const TaskCompletionReasonFailedToRunContainer = "failed to run container"
const TaskCompletionReasonInvalidTransition = "invalid state transition"
const TaskCompletionReasonFailedToFetchResult = "failed to fetch result"

//go:generate counterfeiter -o fake_snapshot/fake_task_processor.go task_processor.go TaskProcessor

type TaskProcessor interface {
	Process(lager.Logger, *TaskSnapshot)
}

type taskProcessor struct {
	bbs               bbs.RepBBS
	containerDelegate ContainerDelegate
	cellID            string
}

func NewTaskProcessor(bbs bbs.RepBBS, containerDelegate ContainerDelegate, cellID string) TaskProcessor {
	return &taskProcessor{
		bbs:               bbs,
		containerDelegate: containerDelegate,
		cellID:            cellID,
	}
}

func (p *taskProcessor) Process(logger lager.Logger, snap *TaskSnapshot) {
	logger = logger.Session("task-processor", lagerDataFromTaskSnapshot(snap))
	err := snap.Validate()
	if err != nil {
		logger.Error("invalid-snapshot", err)
		return
	}

	if snap.Container == nil {
		p.processMissingContainer(logger.Session("process-missing-container"), snap)
		return
	}

	switch snap.Container.State {
	case executor.StateReserved:
		p.processActiveContainer(logger.Session("process-reserved-container"), snap)
	case executor.StateInitializing:
		p.processActiveContainer(logger.Session("process-initializing-container"), snap)
	case executor.StateCreated:
		p.processActiveContainer(logger.Session("process-created-container"), snap)
	case executor.StateRunning:
		p.processActiveContainer(logger.Session("process-running-container"), snap)
	case executor.StateCompleted:
		p.processCompletedContainer(logger.Session("process-completed-container"), snap)
	}
}

func (p *taskProcessor) processMissingContainer(logger lager.Logger, snap *TaskSnapshot) {
	if snap.Task.State == models.TaskStateRunning {
		p.failTask(logger, snap.Task.TaskGuid, TaskCompletionReasonMissingContainer)
	}
}

func (p *taskProcessor) processActiveContainer(logger lager.Logger, snap *TaskSnapshot) {
	ok := p.startTask(logger, snap.Container.Guid)
	if !ok {
		return
	}

	ok = p.containerDelegate.RunContainer(logger, snap.Container.Guid)
	if !ok {
		p.failTask(logger, snap.Container.Guid, TaskCompletionReasonFailedToRunContainer)
	}
}

func (p *taskProcessor) processCompletedContainer(logger lager.Logger, snap *TaskSnapshot) {
	logger.Info("started")

	if snap.Task == nil {
		p.containerDelegate.DeleteContainer(logger, snap.Container.Guid)
	} else {
		logger.Info("completing-task")

		if snap.Task.State == models.TaskStatePending {
			p.failTask(logger, snap.Container.Guid, TaskCompletionReasonInvalidTransition)
		} else if snap.Task.State == models.TaskStateRunning {
			result, err := p.containerDelegate.FetchContainerResult(logger, snap.Container.Guid, snap.Task.ResultFile)
			if err != nil {
				p.failTask(logger, snap.Container.Guid, TaskCompletionReasonFailedToFetchResult)
			} else {
				p.completeTask(logger, snap.Container.Guid, snap.Container.RunResult.Failed, snap.Container.RunResult.FailureReason, result)
			}
		}

		p.containerDelegate.DeleteContainer(logger, snap.Container.Guid)
	}
}

func (p *taskProcessor) startTask(logger lager.Logger, guid string) bool {
	logger.Info("starting-task")
	changed, err := p.bbs.StartTask(logger, guid, p.cellID)
	if err != nil {
		logger.Error("failed-starting-task", err)

		if _, ok := err.(bbserrors.TaskStateTransitionError); ok {
			p.containerDelegate.DeleteContainer(logger, guid)
		} else if err == bbserrors.ErrTaskRunningOnDifferentCell {
			p.containerDelegate.DeleteContainer(logger, guid)
		} else if err == bbserrors.ErrStoreResourceNotFound {
			p.containerDelegate.DeleteContainer(logger, guid)
		}

		return false
	}

	logger.Info("succeeded-starting-task")

	return changed
}

func (p *taskProcessor) completeTask(logger lager.Logger, guid string, failed bool, reason string, result string) {
	err := p.bbs.CompleteTask(logger, guid, p.cellID, failed, reason, result)
	if err != nil {
		logger.Error("failed-completing-task", err)

		if _, ok := err.(bbserrors.TaskStateTransitionError); ok {
			p.failTask(logger, guid, TaskCompletionReasonInvalidTransition)
		}
	} else {
		logger.Info("succeeded-completing-task")
	}
}

func (p *taskProcessor) failTask(logger lager.Logger, guid string, reason string) {
	failLog := logger.Session("failing-task")
	err := p.bbs.FailTask(logger, guid, reason)
	if err != nil {
		failLog.Error("failed", err)
		return
	}

	failLog.Info("succeeded")
}

func lagerDataFromTaskSnapshot(snap *TaskSnapshot) lager.Data {
	data := lager.Data{}
	if snap.Task != nil {
		data["task"] = snap.Task
	}

	if snap.Container != nil {
		data["container-guid"] = snap.Container.Guid
		data["container-state"] = snap.Container.State
	}

	return data
}
