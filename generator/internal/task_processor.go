package internal

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

//go:generate counterfeiter -o fake_internal/fake_task_processor.go task_processor.go TaskProcessor

type TaskProcessor interface {
	Process(lager.Logger, executor.Container)
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

func (p *taskProcessor) Process(logger lager.Logger, container executor.Container) {
	logger = logger.Session("task-processor")

	switch container.State {
	case executor.StateReserved:
		p.processActiveContainer(logger.Session("process-reserved-container"), container)
	case executor.StateInitializing:
		p.processActiveContainer(logger.Session("process-initializing-container"), container)
	case executor.StateCreated:
		p.processActiveContainer(logger.Session("process-created-container"), container)
	case executor.StateRunning:
		p.processActiveContainer(logger.Session("process-running-container"), container)
	case executor.StateCompleted:
		p.processCompletedContainer(logger.Session("process-completed-container"), container)
	}
}

func (p *taskProcessor) processActiveContainer(logger lager.Logger, container executor.Container) {
	logger.Debug("start")
	defer logger.Debug("complete")

	ok := p.startTask(logger, container.Guid)
	if !ok {
		return
	}

	ok = p.containerDelegate.RunContainer(logger, container.Guid)
	if !ok {
		p.failTask(logger, container.Guid, TaskCompletionReasonFailedToRunContainer)
	}
}

func (p *taskProcessor) processCompletedContainer(logger lager.Logger, container executor.Container) {
	logger.Debug("start")
	defer logger.Debug("complete")

	task, ok := p.fetchTask(logger, container.Guid)
	if !ok {
		p.containerDelegate.DeleteContainer(logger, container.Guid)
	} else {
		logger.Info("completing-task")

		if task.State == models.TaskStatePending {
			p.failTask(logger, container.Guid, TaskCompletionReasonInvalidTransition)
		} else if task.State == models.TaskStateRunning {
			result, err := p.containerDelegate.FetchContainerResult(logger, container.Guid, task.ResultFile)
			if err != nil {
				p.failTask(logger, container.Guid, TaskCompletionReasonFailedToFetchResult)
			} else {
				p.completeTask(logger, container.Guid, container.RunResult.Failed, container.RunResult.FailureReason, result)
			}
		}

		p.containerDelegate.DeleteContainer(logger, container.Guid)
	}
}

func (p *taskProcessor) fetchTask(logger lager.Logger, guid string) (models.Task, bool) {
	logger = logger.Session("fetch-task")
	logger.Debug("start")
	defer logger.Debug("complete")

	task, err := p.bbs.TaskByGuid(guid)
	if err != nil {
		logger.Error("failed", err)
		return task, false
	}

	return task, true
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
