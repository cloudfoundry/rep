package internal

import (
	"code.cloudfoundry.org/bbs"
	"code.cloudfoundry.org/bbs/models"
	"code.cloudfoundry.org/ecrhelper"
	"code.cloudfoundry.org/executor"
	"code.cloudfoundry.org/lager/v3"
	"code.cloudfoundry.org/rep"
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
	bbsClient                  bbs.InternalClient
	containerDelegate          ContainerDelegate
	cellID                     string
	stackPathMap               rep.StackPathMap
	layeringMode               string
	runRequestConversionHelper rep.RunRequestConversionHelper
}

func NewTaskProcessor(bbs bbs.InternalClient, containerDelegate ContainerDelegate, cellID string, stackPathMap rep.StackPathMap, layeringMode string) TaskProcessor {
	runRequestConversionHelper := rep.RunRequestConversionHelper{ECRHelper: ecrhelper.NewECRHelper()}

	return &taskProcessor{
		bbsClient:                  bbs,
		containerDelegate:          containerDelegate,
		cellID:                     cellID,
		stackPathMap:               stackPathMap,
		layeringMode:               layeringMode,
		runRequestConversionHelper: runRequestConversionHelper,
	}
}

func (p *taskProcessor) Process(logger lager.Logger, container executor.Container) {
	logger = logger.Session("task-processor", lager.Data{
		"container-guid":  container.Guid,
		"container-state": container.State,
	})

	logger.Debug("starting")
	defer logger.Debug("finished")

	switch container.State {
	case executor.StateReserved:
		logger.Debug("processing-reserved-container")
		p.processActiveContainer(logger, container)
	case executor.StateInitializing:
		logger.Debug("processing-initializing-container")
		p.processActiveContainer(logger, container)
	case executor.StateCreated:
		logger.Debug("processing-created-container")
		p.processActiveContainer(logger, container)
	case executor.StateRunning:
		logger.Debug("processing-running-container")
		p.processActiveContainer(logger, container)
	case executor.StateCompleted:
		logger.Debug("processing-completed-container")
		p.processCompletedContainer(logger, container)
	}
}

func (p *taskProcessor) processActiveContainer(logger lager.Logger, container executor.Container) {
	ok := p.startTask(logger, container.Guid)
	if !ok {
		return
	}

	task, err := p.bbsClient.TaskByGuid(logger, container.Guid)
	if err != nil {
		logger.Error("failed-fetching-task", err)
		return
	}

	runReq, err := p.runRequestConversionHelper.NewRunRequestFromTask(task, p.stackPathMap, p.layeringMode)
	if err != nil {
		logger.Error("failed-to-construct-run-request", err)
		return
	}

	ok = p.containerDelegate.RunContainer(logger, &runReq)
	if !ok {
		err = p.bbsClient.CompleteTask(logger, container.Guid, p.cellID, true, TaskCompletionReasonFailedToRunContainer, "")
		if err != nil {
			logger.Error("failed-completing-task", err)
		}
	}
}

func (p *taskProcessor) processCompletedContainer(logger lager.Logger, container executor.Container) {
	p.completeTask(logger, container)
	p.containerDelegate.DeleteContainer(logger, container.Guid)
}

func (p *taskProcessor) startTask(logger lager.Logger, guid string) bool {
	logger.Info("starting-task")
	changed, err := p.bbsClient.StartTask(logger, guid, p.cellID)
	if err != nil {
		logger.Error("failed-starting-task", err)

		bbsErr := models.ConvertError(err)
		switch bbsErr.Type {
		case models.Error_InvalidStateTransition:
			p.containerDelegate.DeleteContainer(logger, guid)
		case models.Error_ResourceNotFound:
			p.containerDelegate.DeleteContainer(logger, guid)
		}
		return false
	}

	if changed {
		logger.Info("succeeded-starting-task")
	} else {
		logger.Info("task-already-started")
	}

	return changed
}

func (p *taskProcessor) completeTask(logger lager.Logger, container executor.Container) {
	var result string
	var err error

	if container.RunResult.Failed && container.RunResult.Retryable {
		logger.Info("rejecting-task")
		err = p.bbsClient.RejectTask(logger, container.Guid, container.RunResult.FailureReason)
		if err != nil {
			logger.Error("failed-rejecting-task", err)
		}
		return
	}

	resultFile := container.Tags[rep.ResultFileTag]
	if !container.RunResult.Failed && resultFile != "" {
		result, err = p.containerDelegate.FetchContainerResultFile(logger, container.Guid, resultFile)
		if err != nil {
			err = p.bbsClient.CompleteTask(logger, container.Guid, p.cellID, true, TaskCompletionReasonFailedToFetchResult, "")
			if err != nil {
				logger.Error("failed-completing-task", err)
			}
			return
		}
	}

	logger.Info("completing-task")
	err = p.bbsClient.CompleteTask(logger, container.Guid, p.cellID, container.RunResult.Failed, container.RunResult.FailureReason, result)
	if err != nil {
		logger.Error("failed-completing-task", err)

		bbsErr := models.ConvertError(err)
		if bbsErr.Type == models.Error_InvalidStateTransition {
			err = p.bbsClient.CompleteTask(logger, container.Guid, p.cellID, true, TaskCompletionReasonInvalidTransition, "")
			if err != nil {
				logger.Error("failed-completing-task", err)
			}
		}
		return
	}

	logger.Info("succeeded-completing-task")
}
