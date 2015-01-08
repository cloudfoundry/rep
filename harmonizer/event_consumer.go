package harmonizer

import (
	"os"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep/generator"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/operationq"
)

type EventConsumer struct {
	logger         lager.Logger
	executorClient executor.Client
	generator      generator.Generator
	queue          operationq.Queue
}

func NewEventConsumer(
	logger lager.Logger,
	generator generator.Generator,
	queue operationq.Queue,
) *EventConsumer {
	return &EventConsumer{
		logger:    logger,
		generator: generator,
		queue:     queue,
	}
}

func (consumer *EventConsumer) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	logger := consumer.logger.Session("event-consumer")

	stream, err := consumer.generator.OperationStream(logger)
	if err != nil {
		return err
	}

	close(ready)

	for {
		select {
		case op, ok := <-stream:
			if !ok {
				logger.Info("event-stream-closed")
				return nil
			}

			consumer.queue.Push(op)

		case <-signals:
			logger.Info("stopped")
			return nil
		}
	}

	return nil
}
