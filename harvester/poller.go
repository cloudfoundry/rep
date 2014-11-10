package harvester

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

type poller struct {
	pollInterval time.Duration
	timer        timer.Timer

	executorClient executor.Client
	processor      Processor
	bbs            bbs.RepBBS
	executorID     string
	logger         lager.Logger
}

func NewPoller(
	pollInterval time.Duration,
	timer timer.Timer,
	executorClient executor.Client,
	processor Processor,
	bbs bbs.RepBBS,
	executorID string,
	logger lager.Logger,
) ifrit.Runner {
	return &poller{
		pollInterval:   pollInterval,
		timer:          timer,
		executorClient: executorClient,
		processor:      processor,
		bbs:            bbs,
		executorID:     executorID,
		logger:         logger.Session("harvester"),
	}
}

func (poller *poller) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	close(ready)

	poller.logger.Info("started")
	defer poller.logger.Info("stopped")

	ticks := poller.timer.Every(poller.pollInterval)

	containersFilter := executor.Tags{
		LifecycleTag: TaskLifecycle,
	}

	for {
		select {
		case <-ticks:
			containers, err := poller.executorClient.ListContainers(containersFilter)
			if err != nil {
				poller.logger.Error("list-containers", err)
				continue
			}

			containerGuids := make(map[string]struct{})
			for _, container := range containers {
				containerGuids[container.Guid] = struct{}{}
				if container.State == executor.StateCompleted {
					poller.processor.Process(container)
				}
			}

			tasks, err := poller.bbs.GetAllTasksByExecutorID(poller.executorID)
			if err != nil {
				poller.logger.Error("get-tasks", err)
				continue
			}

			for _, task := range tasks {
				if _, ok := containerGuids[task.TaskGuid]; ok {
					continue
				}
				if task.State != models.TaskStateClaimed && task.State != models.TaskStateRunning {
					continue
				}
				err = poller.bbs.CompleteTask(task.TaskGuid, true, "task container no longer exists", "")
				if err != nil {
					poller.logger.Error("complete-task", err)
				}
			}

		case <-signals:
			return nil
		}
	}
}
