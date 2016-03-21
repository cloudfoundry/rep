package harmonizer

import (
	"os"
	"time"

	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context"
	"github.com/cloudfoundry-incubator/rep/generator"
	"github.com/cloudfoundry-incubator/runtime-schema/metric"
	"github.com/pivotal-golang/clock"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/operationq"
)

const repBulkSyncDuration = metric.Duration("RepBulkSyncDuration")

type Bulker struct {
	logger lager.Logger

	pollInterval           time.Duration
	evacuationPollInterval time.Duration
	evacuationNotifier     evacuation_context.EvacuationNotifier
	clock                  clock.Clock
	generator              generator.Generator
	queue                  operationq.Queue
}

func NewBulker(
	logger lager.Logger,
	pollInterval time.Duration,
	evacuationPollInterval time.Duration,
	evacuationNotifier evacuation_context.EvacuationNotifier,
	clock clock.Clock,
	generator generator.Generator,
	queue operationq.Queue,
) *Bulker {
	return &Bulker{
		logger: logger,

		pollInterval:           pollInterval,
		evacuationPollInterval: evacuationPollInterval,
		evacuationNotifier:     evacuationNotifier,
		clock:                  clock,
		generator:              generator,
		queue:                  queue,
	}
}

func (b *Bulker) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	evacuateNotify := b.evacuationNotifier.EvacuateNotify()
	close(ready)

	logger := b.logger.Session("running-bulker")

	logger.Info("starting", lager.Data{
		"interval": b.pollInterval.String(),
	})
	defer logger.Info("finished")

	interval := b.pollInterval

	timer := b.clock.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-timer.C():

		case <-evacuateNotify:
			timer.Stop()
			evacuateNotify = nil

			logger.Info("notified-of-evacuation")
			interval = b.evacuationPollInterval

		case signal := <-signals:
			logger.Info("received-signal", lager.Data{"signal": signal.String()})
			return nil
		}

		b.sync(logger)
		timer.Reset(interval)
	}
}

func (b *Bulker) sync(logger lager.Logger) {
	logger = logger.Session("sync")

	logger.Info("starting")
	defer logger.Info("finished")

	startTime := b.clock.Now()

	ops, batchError := b.generator.BatchOperations(logger)

	endTime := b.clock.Now()

	sendError := repBulkSyncDuration.Send(endTime.Sub(startTime))
	if sendError != nil {
		logger.Error("failed-to-send-rep-bulk-sync-duration-metric", sendError)
	}

	if batchError != nil {
		logger.Error("failed-to-generate-operations", batchError)
		return
	}

	for _, operation := range ops {
		b.queue.Push(operation)
	}
}
