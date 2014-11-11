package reaper

import (
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/timer"
	"github.com/tedsuo/ifrit"
)

type taskReaper struct {
	pollInterval time.Duration
	timer        timer.Timer

	executorID     string
	bbs            bbs.RepBBS
	executorClient executor.Client
	logger         lager.Logger
}

func NewTaskReaper(
	pollInterval time.Duration,
	timer timer.Timer,
	executorID string,
	bbs bbs.RepBBS,
	executorClient executor.Client,
	logger lager.Logger,
) ifrit.Runner {
	return &taskReaper{
		pollInterval:   pollInterval,
		timer:          timer,
		executorID:     executorID,
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

			r.logger.Info("reaper-getting-tasks-by-executor-id", lager.Data{"executor-id": r.executorID})
			tasks, err := r.bbs.GetAllTasksByExecutorID(r.executorID)
			if err != nil {
				r.logger.Error("reaper-failed-to-get-tasks-by-executor-id", err, lager.Data{"executor-id": r.executorID})
				continue
			}

			for _, task := range tasks {
				if task.State != models.TaskStateClaimed && task.State != models.TaskStateRunning {
					continue
				}

				r.logger.Info("reaper-finding-container-for-task", lager.Data{"task": task})
				_, err = r.executorClient.GetContainer(task.TaskGuid)

				if err == executor.ErrContainerNotFound {
					r.logger.Info("reaper-found-no-container-for-task", lager.Data{"task": task})

					r.logger.Info("reaper-marking-containerless-task-as-failed", lager.Data{"task": task})
					err = r.bbs.CompleteTask(task.TaskGuid, true, "task container no longer exists", "")
					if err != nil {
						r.logger.Error("reaper-failed-to-mark-containerless-task-as-failed", err, lager.Data{"task": task})
					}
				} else if err != nil {
					r.logger.Error("reaper-failed-to-determine-container-existence-for-task", err, lager.Data{"task": task})
				}
			}

			r.logger.Info("reaper-exiting-loop")

		case <-signals:
			return nil
		}
	}
}
