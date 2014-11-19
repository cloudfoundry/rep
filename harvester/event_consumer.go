package harvester

import (
	"os"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/pivotal-golang/lager"
	"github.com/tedsuo/ifrit"
)

type eventConsumer struct {
	logger         lager.Logger
	executorClient executor.Client
	processor      Processor
}

func NewEventConsumer(
	logger lager.Logger,
	executorClient executor.Client,
	processor Processor,
) ifrit.Runner {
	return &eventConsumer{
		logger:         logger,
		executorClient: executorClient,
		processor:      processor,
	}
}

func (consumer *eventConsumer) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	logger := consumer.logger.Session("event-consumer")

	events, err := consumer.executorClient.SubscribeToEvents()
	if err != nil {
		return err
	}

	close(ready)

	for {
		select {
		case e, ok := <-events:
			if !ok {
				logger.Info("event-stream-closed")
				return nil
			}

			logger.Info("event-received", lager.Data{
				"event-type": e.EventType(),
			})

			switch event := e.(type) {
			case executor.ContainerCompleteEvent:
				if event.Container.Tags == nil {
					continue
				}

				lifecycle := event.Container.Tags[rep.LifecycleTag]
				if lifecycle == rep.TaskLifecycle || lifecycle == rep.LRPLifecycle {
					consumer.processor.Process(event.Container)
				}

			case executor.ContainerHealthEvent:
				if event.Container.Tags == nil {
					continue
				}

				if event.Container.Tags[rep.LifecycleTag] == rep.LRPLifecycle {
					consumer.processor.Process(event.Container)
				}
			}

		case <-signals:
			logger.Info("event-consumer-stopped")
			return nil
		}
	}

	return nil
}
