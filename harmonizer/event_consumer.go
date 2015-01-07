package harmonizer

import (
	"os"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep/snapshot"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/operationq"
)

type EventConsumer struct {
	logger         lager.Logger
	executorClient executor.Client
	generator      snapshot.Generator
	queue          operationq.Queue
}

func NewEventConsumer(
	logger lager.Logger,
	executorClient executor.Client,
	generator snapshot.Generator,
	queue operationq.Queue,
) *EventConsumer {
	return &EventConsumer{
		logger:         logger,
		executorClient: executorClient,
		generator:      generator,
		queue:          queue,
	}
}

func (consumer *EventConsumer) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
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

			consumer.processEvent(e, logger.Session("event", lager.Data{
				"event-type": e.EventType(),
			}))

		case <-signals:
			logger.Info("stopped")
			return nil
		}
	}

	return nil
}

func (consumer *EventConsumer) processEvent(e executor.Event, logger lager.Logger) {
	sess := logger.Session("process")

	sess.Debug("start")
	defer sess.Debug("done")

	switch event := e.(type) {
	case executor.LifecycleEvent:
		op, err := consumer.generator.ContainerOperation(logger, event.Container())
		if err != nil {
			sess.Error("failed-to-generate-operation", err)
			return
		}

		consumer.queue.Push(op)

	default:
		sess.Debug("ignoring")
	}
}
