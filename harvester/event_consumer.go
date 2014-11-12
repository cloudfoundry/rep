package harvester

import (
	"os"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/tedsuo/ifrit"
)

type eventConsumer struct {
	executorClient executor.Client
	processor      Processor
}

func NewEventConsumer(
	executorClient executor.Client,
	processor Processor,
) ifrit.Runner {
	return &eventConsumer{
		executorClient: executorClient,
		processor:      processor,
	}
}

func (consumer *eventConsumer) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	events, err := consumer.executorClient.SubscribeToEvents()
	if err != nil {
		return err
	}

	close(ready)

	for {
		select {
		case e, ok := <-events:
			if !ok {
				return nil
			}

			switch event := e.(type) {
			case executor.ContainerCompleteEvent:
				if event.Container.Tags == nil {
					continue
				}

				if event.Container.Tags[rep.LifecycleTag] != rep.TaskLifecycle {
					continue
				}

				go consumer.processor.Process(event.Container)
			}

		case <-signals:
			return nil
		}
	}

	return nil
}
