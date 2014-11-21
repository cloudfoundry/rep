package harvester

import (
	"github.com/cloudfoundry-incubator/rep/gatherer"
	"github.com/pivotal-golang/lager"
)

type BulkContainerProcessor struct {
	processor Processor
	logger    lager.Logger
}

func NewBulkContainerProcessor(
	processor Processor,
	logger lager.Logger,
) *BulkContainerProcessor {
	return &BulkContainerProcessor{
		processor: processor,
		logger:    logger.Session("bulk-container-processor"),
	}
}

func (bulk *BulkContainerProcessor) Process(snapshot gatherer.Snapshot) {
	bulk.logger.Info("started")

	containers := snapshot.ListContainers(nil)

	for _, container := range containers {
		bulk.processor.Process(container)
	}

	bulk.logger.Info("stopped")
}
