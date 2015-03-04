package internal

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/bbserrors"
	"github.com/pivotal-golang/lager"
)

type ordinaryLRPProcessor struct {
	bbs               bbs.RepBBS
	containerDelegate ContainerDelegate
	cellID            string
}

func newOrdinaryLRPProcessor(
	bbs bbs.RepBBS,
	containerDelegate ContainerDelegate,
	cellID string,
) LRPProcessor {
	return &ordinaryLRPProcessor{
		bbs:               bbs,
		containerDelegate: containerDelegate,
		cellID:            cellID,
	}
}

func (p *ordinaryLRPProcessor) Process(logger lager.Logger, container executor.Container) {
	logger = logger.Session("ordinary-lrp-processor", lager.Data{
		"container-guid":  container.Guid,
		"container-state": container.State,
	})
	logger.Debug("starting")
	defer logger.Debug("finished")

	lrpKey, err := rep.ActualLRPKeyFromContainer(container)
	if err != nil {
		logger.Error("failed-to-generate-lrp-key", err)
		return
	}
	logger.WithData(lager.Data{"lrp-key": lrpKey})

	containerKey, err := rep.ActualLRPContainerKeyFromContainer(container, p.cellID)
	if err != nil {
		logger.Error("failed-to-generate-container-key", err)
		return
	}
	logger.WithData(lager.Data{"container-key": containerKey})

	lrpContainer := newLRPContainer(lrpKey, containerKey, container)
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

func (p *ordinaryLRPProcessor) processReservedContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-reserved-container")
	ok := p.claimLRPContainer(logger, lrpContainer)
	if !ok {
		return
	}

	ok = p.containerDelegate.RunContainer(logger, lrpContainer.Guid)
	if !ok {
		p.bbs.RemoveActualLRP(logger, lrpContainer.ActualLRPKey, lrpContainer.ActualLRPContainerKey)
		return
	}
}

func (p *ordinaryLRPProcessor) processInitializingContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-initializing-container")
	p.claimLRPContainer(logger, lrpContainer)
}

func (p *ordinaryLRPProcessor) processCreatedContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-created-container")
	p.claimLRPContainer(logger, lrpContainer)
}

func (p *ordinaryLRPProcessor) processRunningContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-running-container")

	logger.Debug("extracting-net-info-from-container")
	netInfo, err := rep.ActualLRPNetInfoFromContainer(lrpContainer.Container)
	if err != nil {
		logger.Error("failed-extracting-net-info-from-container", err)
		return
	}
	logger.Debug("succeeded-extracting-net-info-from-container")

	err = p.bbs.StartActualLRP(logger, lrpContainer.ActualLRPKey, lrpContainer.ActualLRPContainerKey, netInfo)
	if err == bbserrors.ErrActualLRPCannotBeStarted {
		p.containerDelegate.StopContainer(logger, lrpContainer.Guid)
	}
}

func (p *ordinaryLRPProcessor) processCompletedContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-completed-container")

	if lrpContainer.RunResult.Stopped {
		p.bbs.RemoveActualLRP(logger, lrpContainer.ActualLRPKey, lrpContainer.ActualLRPContainerKey)
	} else {
		p.bbs.CrashActualLRP(logger, lrpContainer.ActualLRPKey, lrpContainer.ActualLRPContainerKey)
	}

	p.containerDelegate.DeleteContainer(logger, lrpContainer.Guid)
}

func (p *ordinaryLRPProcessor) processInvalidContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-invalid-container")
	logger.Error("not-processing-container-in-invalid-state", nil)
}

func (p *ordinaryLRPProcessor) claimLRPContainer(logger lager.Logger, lrpContainer *lrpContainer) bool {
	err := p.bbs.ClaimActualLRP(logger, lrpContainer.ActualLRPKey, lrpContainer.ActualLRPContainerKey)
	switch err {
	case nil:
		return true
	case bbserrors.ErrActualLRPCannotBeClaimed:
		p.containerDelegate.DeleteContainer(logger, lrpContainer.Guid)
		return false
	default:
		return false
	}
}
