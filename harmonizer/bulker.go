package harmonizer

import (
	"os"
	"time"

	"github.com/cloudfoundry-incubator/rep/snapshot"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/operationq"
)

type Bulker struct {
	logger lager.Logger

	pollInterval time.Duration
	timeProvider timeprovider.TimeProvider
	generator    snapshot.Generator
	queue        operationq.Queue
}

func NewBulker(
	logger lager.Logger,
	pollInterval time.Duration,
	timeProvider timeprovider.TimeProvider,
	generator snapshot.Generator,
	queue operationq.Queue,
) *Bulker {
	return &Bulker{
		logger: logger,

		pollInterval: pollInterval,
		timeProvider: timeProvider,
		generator:    generator,
		queue:        queue,
	}
}

func (b *Bulker) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	close(ready)

	logger := b.logger.Session("bulker")

	logger.Info("starting", lager.Data{"interval": b.pollInterval.String()})
	defer logger.Info("completed")

	ticker := b.timeProvider.NewTicker(b.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C():
			b.sync(logger.Session("sync"))

		case <-signals:
			return nil
		}
	}
}

func (b *Bulker) sync(logger lager.Logger) {
	logger.Info("start")
	defer logger.Info("done")

	ops, err := b.generator.BatchOperations(logger)
	if err != nil {
		logger.Error("failed-to-generate-operations", err)
		return
	}

	for _, operation := range ops {
		b.queue.Push(operation)
	}
}
