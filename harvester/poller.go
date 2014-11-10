package harvester

import (
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
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

			for _, container := range containers {
				if container.State == executor.StateCompleted {
					poller.processor.Process(container)
				}
			}

		case <-signals:
			return nil
		}
	}
}
