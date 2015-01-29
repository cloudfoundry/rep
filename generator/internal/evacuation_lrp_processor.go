package internal

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/pivotal-golang/lager"
)

type evacuationLRPProcessor struct {
	bbs               bbs.RepBBS
	containerDelegate ContainerDelegate
	cellID            string
	ordinaryProcessor LRPProcessor
}

func newEvacuationLRPProcessor(bbs bbs.RepBBS, containerDelegate ContainerDelegate, cellID string, ordinaryProcessor LRPProcessor) LRPProcessor {
	return &evacuationLRPProcessor{
		bbs:               bbs,
		containerDelegate: containerDelegate,
		cellID:            cellID,
		ordinaryProcessor: ordinaryProcessor,
	}
}

func (p *evacuationLRPProcessor) Process(logger lager.Logger, container executor.Container) {
	logger = logger.Session("evacuation-lrp-processor")
	logger.Debug("start")

	lrpKey, err := rep.ActualLRPKeyFromContainer(container)
	if err != nil {
		logger.Error("failed-to-generate-lrp-key", err)
		return
	}

	containerKey, err := rep.ActualLRPContainerKeyFromContainer(container, p.cellID)
	if err != nil {
		logger.Error("failed-to-generate-container-key", err)
		return
	}

	lrpContainer := newLRPContainer(lrpKey, containerKey, container)

	switch lrpContainer.Container.State {
	case executor.StateReserved, executor.StateInitializing, executor.StateCreated:
		err := p.bbs.EvacuateActualLRP(logger, lrpContainer.ActualLRPKey, lrpContainer.ActualLRPContainerKey)
		if err != nil {
			logger.Error("failed-to-unclaim-actual-lrp", err, lager.Data{"lrp-key": lrpContainer.ActualLRPKey})
		}

		p.containerDelegate.DeleteContainer(logger, lrpContainer.Container.Guid)
	default:
		p.ordinaryProcessor.Process(logger, container)
	}
}
