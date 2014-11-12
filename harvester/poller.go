package harvester

import (
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/timer"
	"github.com/tedsuo/ifrit"
)

type poller struct {
	pollInterval   time.Duration
	timer          timer.Timer
	executorClient executor.Client
	processor      Processor
	logger         lager.Logger
}

func NewPoller(
	pollInterval time.Duration,
	timer timer.Timer,
	executorClient executor.Client,
	processor Processor,
	logger lager.Logger,
) ifrit.Runner {
	return &poller{
		pollInterval:   pollInterval,
		timer:          timer,
		executorClient: executorClient,
		processor:      processor,
		logger:         logger.Session("poller"),
	}
}

func (poller *poller) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	close(ready)

	ticks := poller.timer.Every(poller.pollInterval)

	for {
		select {
		case <-ticks:
			containers, err := poller.executorClient.ListContainers(nil)
			if err != nil {
				poller.logger.Error("list-containers-failed", err)
				continue
			}

			for _, container := range containers {
				poller.processor.Process(container)
			}

		case <-signals:
			return nil
		}
	}
}
