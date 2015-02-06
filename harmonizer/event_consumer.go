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
	logger.Info("starting")
	defer logger.Info("finished")

	logger.Info("subscribing-to-operation-stream")
	stream, err := consumer.generator.OperationStream(logger)
	if err != nil {
		logger.Error("failed-subscribing-to-operation-stream", err)
		return err
	}
	logger.Info("succeeded-subscribing-to-operation-stream")

	close(ready)
	logger.Info("started")

	for {
		select {
		case op, ok := <-stream:
			if !ok {
				logger.Info("event-stream-closed")
				return nil
			}

			consumer.queue.Push(op)

		case signal := <-signals:
			logger.Info("received-signal", lager.Data{"signal": signal.String()})
			return nil
		}
	}

	return nil
}
