package tallyman

import (
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/pivotal-golang/timer"
	"github.com/tedsuo/ifrit"
)

type poller struct {
	pollInterval time.Duration
	timer        timer.Timer

	executorClient executor.Client
	processor      Processor
}

func NewPoller(
	pollInterval time.Duration,
	timer timer.Timer,
	executorClient executor.Client,
	processor Processor,
) ifrit.Runner {
	return &poller{
		pollInterval:   pollInterval,
		timer:          timer,
		executorClient: executorClient,
		processor:      processor,
	}
}

func (poller *poller) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	close(ready)

	ticks := poller.timer.Every(poller.pollInterval)

	containersFilter := executor.Tags{
		LifecycleTag: TaskLifecycle,
	}

	for {
		select {
		case <-ticks:
			containers, err := poller.executorClient.ListContainers(containersFilter)
			if err != nil {
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
