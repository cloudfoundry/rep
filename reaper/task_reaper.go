package reaper

import (
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/storeadapter"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/timer"
	"github.com/tedsuo/ifrit"
)

type taskReaper struct {
	pollInterval time.Duration
	timer        timer.Timer

	cellID         string
	bbs            bbs.RepBBS
	executorClient executor.Client
	logger         lager.Logger
}

func NewTaskReaper(
	pollInterval time.Duration,
	timer timer.Timer,
	cellID string,
	bbs bbs.RepBBS,
	executorClient executor.Client,
	logger lager.Logger,
) ifrit.Runner {
	return &taskReaper{
		pollInterval:   pollInterval,
		timer:          timer,
		cellID:         cellID,
		bbs:            bbs,
		executorClient: executorClient,
		logger:         logger,
	}
}

func (r *taskReaper) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	close(ready)

	ticks := r.timer.Every(r.pollInterval)

	for {
		select {
		case <-ticks:
			r.logger.Info("reaper-entering-loop")

			r.markTasksWithMissingContainersAsCompleted()
			r.deleteCompletedContainers()

			r.logger.Info("reaper-exiting-loop")

		case <-signals:
			return nil
		}
	}
}

func (r *taskReaper) markTasksWithMissingContainersAsCompleted() {
	r.logger.Info("reaper-getting-tasks-by-cell-id", lager.Data{"cell-id": r.cellID})
	tasks, err := r.bbs.TasksByCellID(r.cellID)
	if err != nil {
		r.logger.Error("reaper-failed-to-get-tasks-by-cell-id", err, lager.Data{"cell-id": r.cellID})
		return
	}

	for _, task := range tasks {
		if task.State != models.TaskStateClaimed && task.State != models.TaskStateRunning {
			continue
		}

		_, err := r.executorClient.GetContainer(task.TaskGuid)

		if err == executor.ErrContainerNotFound {
			r.logger.Info("reaper-marking-containerless-task-as-failed", lager.Data{"task": task})
			err := r.bbs.CompleteTask(task.TaskGuid, true, "task container no longer exists", "")
			if err != nil {
				r.logger.Error("reaper-failed-to-mark-containerless-task-as-failed", err, lager.Data{"task": task})
			}
		} else if err != nil {
			r.logger.Error("reaper-failed-to-determine-container-existence-for-task", err, lager.Data{"task": task})
		}

	}
}

func (r *taskReaper) deleteCompletedContainers() {
	containers, err := r.executorClient.ListContainers(executor.Tags{
		rep.LifecycleTag: rep.TaskLifecycle,
	})
	if err != nil {
		r.logger.Error("reaper-failed-to-list-containers", err)
		return
	}

	for _, container := range containers {
		task, err := r.bbs.TaskByGuid(container.Guid)

		taskExists := true
		if err == storeadapter.ErrorKeyNotFound {
			taskExists = false
		} else if err != nil {
			r.logger.Error("reaper-failed-to-get-task", err)
			continue
		} else if task == nil {
			taskExists = false
		}

		if !taskExists || task.State == models.TaskStateCompleted || task.State == models.TaskStateResolving {
			r.logger.Info("reaper-deleting-container", lager.Data{"task-exists": taskExists, "task": task})
			err := r.executorClient.DeleteContainer(container.Guid)
			if err != nil {
				r.logger.Error("reaper-failed-to-delete-container", err, lager.Data{"task": task})
			}
		}
	}
}
