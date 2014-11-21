package reaper

import (
	"github.com/cloudfoundry-incubator/rep/gatherer"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

type TaskCompleter struct {
	bbs    bbs.RepBBS
	logger lager.Logger
}

func NewTaskCompleter(
	bbs bbs.RepBBS,
	logger lager.Logger,
) *TaskCompleter {
	return &TaskCompleter{
		bbs:    bbs,
		logger: logger.Session("task-completer"),
	}
}

func (r *TaskCompleter) Process(snapshot gatherer.Snapshot) {
	r.logger.Info("started")

	tasks := snapshot.Tasks()

	for _, task := range tasks {
		if task.State != models.TaskStateClaimed && task.State != models.TaskStateRunning {
			continue
		}

		_, ok := snapshot.GetContainer(task.TaskGuid)

		if !ok {
			r.logger.Info("task-with-no-corresponding-container", lager.Data{"task-guid": task.TaskGuid})
			err := r.bbs.CompleteTask(task.TaskGuid, true, "task container no longer exists", "")
			if err != nil {
				r.logger.Error("failed-to-complete-task-with-no-corresponding-container", err, lager.Data{"task-guid": task.TaskGuid})
			}
		}
	}

	r.logger.Info("stopped")
}
