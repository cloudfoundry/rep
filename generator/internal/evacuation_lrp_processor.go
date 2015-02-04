package internal

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/bbserrors"
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
		err := p.bbs.EvacuateClaimedActualLRP(logger, lrpContainer.ActualLRPKey, lrpContainer.ActualLRPContainerKey)
		if err != nil {
			logger.Error("failed-to-unclaim-actual-lrp", err, lager.Data{"lrp-key": lrpContainer.ActualLRPKey})
		}

		p.containerDelegate.DeleteContainer(logger, lrpContainer.Container.Guid)
	case executor.StateRunning:
		netInfo, err := rep.ActualLRPNetInfoFromContainer(lrpContainer.Container)
		if err != nil {
			logger.Error("failed-extracting-net-info-from-container", err)
			return
		}

		err = p.bbs.EvacuateRunningActualLRP(logger, lrpContainer.ActualLRPKey, lrpContainer.ActualLRPContainerKey, netInfo, p.evacuationTTLInSeconds)
		if err != nil {
			logger.Error("failed-to-evacuate-running-actual-lrp", err, lager.Data{"lrp-key": lrpContainer.ActualLRPKey})
		}
		if err == bbserrors.ErrActualLRPCannotBeEvacuated {
			p.containerDelegate.DeleteContainer(logger, lrpContainer.Container.Guid)
		}

	case executor.StateCompleted:
		if lrpContainer.RunResult.Stopped {
			err := p.bbs.EvacuateStoppedActualLRP(logger, lrpContainer.ActualLRPKey, lrpContainer.ActualLRPContainerKey)
			if err != nil {
				logger.Error("failed-to-evacuate-stopped-actual-lrp", err, lager.Data{"lrp-key": lrpContainer.ActualLRPKey})
			}
		} else {
			err := p.bbs.EvacuateCrashedActualLRP(logger, lrpContainer.ActualLRPKey, lrpContainer.ActualLRPContainerKey)
			if err != nil {
				logger.Error("failed-to-evacuate-crashed-actual-lrp", err, lager.Data{"lrp-key": lrpContainer.ActualLRPKey})
			}
		}
		p.containerDelegate.DeleteContainer(logger, lrpContainer.Container.Guid)

	default:
		logger.Error("not-processing-container-in-invalid-state", nil)
	}
}
