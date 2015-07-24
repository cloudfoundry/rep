package internal

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/pivotal-golang/lager"
)

type evacuationLRPProcessor struct {
	bbs                    bbs.RepBBS
	containerDelegate      ContainerDelegate
	cellID                 string
	evacuationTTLInSeconds uint64
}

func newEvacuationLRPProcessor(bbs bbs.RepBBS, containerDelegate ContainerDelegate, cellID string, evacuationTTLInSeconds uint64) LRPProcessor {
	return &evacuationLRPProcessor{
		bbs:                    bbs,
		containerDelegate:      containerDelegate,
		cellID:                 cellID,
		evacuationTTLInSeconds: evacuationTTLInSeconds,
	}
}

func (p *evacuationLRPProcessor) Process(logger lager.Logger, container executor.Container) {
	logger = logger.Session("evacuation-lrp-processor", lager.Data{
		"container-guid":  container.Guid,
		"container-state": container.State,
	})
	logger.Debug("start")

	lrpKey, err := rep.ActualLRPKeyFromContainer(container)
	if err != nil {
		logger.Error("failed-to-generate-lrp-key", err)
		return
	}

	instanceKey, err := rep.ActualLRPInstanceKeyFromContainer(container, p.cellID)
	if err != nil {
		logger.Error("failed-to-generate-instance-key", err)
		return
	}

	lrpContainer := newLRPContainer(lrpKey, instanceKey, container)

	switch lrpContainer.Container.State {
	case executor.StateReserved:
		p.processReservedContainer(logger, lrpContainer)
	case executor.StateInitializing:
		p.processInitializingContainer(logger, lrpContainer)
	case executor.StateCreated:
		p.processCreatedContainer(logger, lrpContainer)
	case executor.StateRunning:
		p.processRunningContainer(logger, lrpContainer)
	case executor.StateCompleted:
		p.processCompletedContainer(logger, lrpContainer)
	default:
		p.processInvalidContainer(logger, lrpContainer)
	}
}

func (p *evacuationLRPProcessor) processReservedContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-reserved-container")
	p.evacuateClaimedLRPContainer(logger, lrpContainer)
}

func (p *evacuationLRPProcessor) processInitializingContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-initializing-container")
	p.evacuateClaimedLRPContainer(logger, lrpContainer)
}

func (p *evacuationLRPProcessor) processCreatedContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-created-container")
	p.evacuateClaimedLRPContainer(logger, lrpContainer)
}

func (p *evacuationLRPProcessor) processRunningContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-running-container")

	logger.Debug("extracting-net-info-from-container")
	netInfo, err := rep.LegacyActualLRPNetInfoFromContainer(lrpContainer.Container)
	if err != nil {
		logger.Error("failed-extracting-net-info-from-container", err)
		return
	}
	logger.Debug("succeeded-extracting-net-info-from-container")

	retainment, err := p.bbs.EvacuateRunningActualLRP(logger, lrpContainer.ActualLRPKey, lrpContainer.ActualLRPInstanceKey, netInfo, p.evacuationTTLInSeconds)
	if retainment == shared.DeleteContainer {
		p.containerDelegate.DeleteContainer(logger, lrpContainer.Container.Guid)
	} else if err != nil {
		logger.Error("failed-to-evacuate-running-actual-lrp", err, lager.Data{"lrp-key": lrpContainer.ActualLRPKey})
	}
}

func (p *evacuationLRPProcessor) processCompletedContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-completed-container")

	if lrpContainer.RunResult.Stopped {
		_, err := p.bbs.EvacuateStoppedActualLRP(logger, lrpContainer.ActualLRPKey, lrpContainer.ActualLRPInstanceKey)
		if err != nil {
			logger.Error("failed-to-evacuate-stopped-actual-lrp", err, lager.Data{"lrp-key": lrpContainer.ActualLRPKey})
		}
	} else {
		_, err := p.bbs.EvacuateCrashedActualLRP(logger, lrpContainer.ActualLRPKey, lrpContainer.ActualLRPInstanceKey, lrpContainer.RunResult.FailureReason)
		if err != nil {
			logger.Error("failed-to-evacuate-crashed-actual-lrp", err, lager.Data{"lrp-key": lrpContainer.ActualLRPKey})
		}
	}

	p.containerDelegate.DeleteContainer(logger, lrpContainer.Guid)
}

func (p *evacuationLRPProcessor) processInvalidContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-invalid-container")
	logger.Error("not-processing-container-in-invalid-state", nil)
}

func (p *evacuationLRPProcessor) evacuateClaimedLRPContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	_, err := p.bbs.EvacuateClaimedActualLRP(logger, lrpContainer.ActualLRPKey, lrpContainer.ActualLRPInstanceKey)
	if err != nil {
		logger.Error("failed-to-unclaim-actual-lrp", err, lager.Data{"lrp-key": lrpContainer.ActualLRPKey})
	}

	p.containerDelegate.DeleteContainer(logger, lrpContainer.Container.Guid)
}
