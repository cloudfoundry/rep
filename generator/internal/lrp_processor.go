package internal

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/evacuation"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/bbserrors"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

type lrpContainer struct {
	models.ActualLRPKey
	models.ActualLRPContainerKey
	executor.Container
}

func newLRPContainer(lrpKey models.ActualLRPKey, containerKey models.ActualLRPContainerKey, container executor.Container) *lrpContainer {
	return &lrpContainer{
		ActualLRPKey:          lrpKey,
		ActualLRPContainerKey: containerKey,
		Container:             container,
	}
}

//go:generate counterfeiter -o fake_internal/fake_lrp_processor.go lrp_processor.go LRPProcessor

type LRPProcessor interface {
	Process(lager.Logger, executor.Container)
}

type lrpProcessor struct {
	bbs               bbs.RepBBS
	containerDelegate ContainerDelegate
	cellID            string
	evacuationContext evacuation.EvacuationContext
}

func NewLRPProcessor(bbs bbs.RepBBS,
	containerDelegate ContainerDelegate,
	cellID string,
	evacuationContext evacuation.EvacuationContext,
) LRPProcessor {
	return &lrpProcessor{
		bbs:               bbs,
		containerDelegate: containerDelegate,
		cellID:            cellID,
		evacuationContext: evacuationContext,
	}
}

func (p *lrpProcessor) Process(logger lager.Logger, container executor.Container) {
	logger = logger.Session("lrp-processor")
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
	case executor.StateReserved:
		if !p.evacuationContext.Evacuating() {
			p.processReservedContainer(logger, lrpContainer)
		}
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

func (p *lrpProcessor) processReservedContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-reserved-container")
	ok := p.claimLRPContainer(logger, lrpContainer)
	if !ok {
		return
	}

	ok = p.containerDelegate.RunContainer(logger, lrpContainer.Guid)
	if !ok {
		p.bbs.RemoveActualLRP(lrpContainer.ActualLRPKey, lrpContainer.ActualLRPContainerKey, logger)
		return
	}
}

func (p *lrpProcessor) processInitializingContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-initializing-container")
	p.claimLRPContainer(logger, lrpContainer)
}

func (p *lrpProcessor) processCreatedContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-created-container")
	p.claimLRPContainer(logger, lrpContainer)
}

func (p *lrpProcessor) processRunningContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-running-container")

	logger.Debug("extracting-net-info-from-container")
	netInfo, err := rep.ActualLRPNetInfoFromContainer(lrpContainer.Container)
	if err != nil {
		logger.Error("failed-extracting-net-info-from-container", err)
		return
	}
	logger.Debug("succeeded-extracting-net-info-from-container")

	err = p.bbs.StartActualLRP(lrpContainer.ActualLRPKey, lrpContainer.ActualLRPContainerKey, netInfo, logger)
	if err == bbserrors.ErrActualLRPCannotBeStarted {
		p.containerDelegate.StopContainer(logger, lrpContainer.Guid)
	}
}

func (p *lrpProcessor) processCompletedContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-completed-container")

	if lrpContainer.RunResult.Stopped {
		p.bbs.RemoveActualLRP(lrpContainer.ActualLRPKey, lrpContainer.ActualLRPContainerKey, logger)
	} else {
		p.bbs.CrashActualLRP(lrpContainer.ActualLRPKey, lrpContainer.ActualLRPContainerKey, logger)
	}

	p.containerDelegate.DeleteContainer(logger, lrpContainer.Guid)
}

func (p *lrpProcessor) processInvalidContainer(logger lager.Logger, lrpContainer *lrpContainer) {
	logger = logger.Session("process-invalid-container")
	logger.Error("not-processing-container-in-invalid-state", nil)
}

func (p *lrpProcessor) claimLRPContainer(logger lager.Logger, lrpContainer *lrpContainer) bool {
	err := p.bbs.ClaimActualLRP(lrpContainer.ActualLRPKey, lrpContainer.ActualLRPContainerKey, logger)
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
