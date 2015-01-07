package snapshot

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/bbserrors"
	"github.com/pivotal-golang/lager"
)

//go:generate counterfeiter -o fake_snapshot/fake_lrp_processor.go lrp_processor.go LRPProcessor

type LRPProcessor interface {
	Process(lager.Logger, *LRPSnapshot)
}

type lrpProcessor struct {
	bbs               bbs.RepBBS
	containerDelegate ContainerDelegate
}

func NewLRPProcessor(bbs bbs.RepBBS, containerDelegate ContainerDelegate) LRPProcessor {
	return &lrpProcessor{
		bbs:               bbs,
		containerDelegate: containerDelegate,
	}
}

func (p *lrpProcessor) Process(logger lager.Logger, snap *LRPSnapshot) {
	logger = logger.Session("lrp-processor", lagerDataFromLRPSnapshot(snap))
	if err := snap.Validate(); err != nil {
		logger.Error("invalid-snapshot", err)
		return
	}

	if snap.Container == nil {
		p.processMissingContainer(snap, logger)
		return
	}

	switch snap.Container.State {
	case executor.StateReserved:
		p.processReservedContainer(snap, logger)
	case executor.StateInitializing:
		p.processInitializingContainer(logger, snap)
	case executor.StateCreated:
		p.processCreatedContainer(logger, snap)
	case executor.StateRunning:
		p.processRunningContainer(logger, snap)
	case executor.StateCompleted:
		p.processCompletedContainer(logger, snap)
	default:
		p.processInvalidContainer(logger, snap)
	}
}

func (p *lrpProcessor) processMissingContainer(snap *LRPSnapshot, logger lager.Logger) {
	logger = logger.Session("process-missing-container")
	p.bbs.RemoveActualLRP(snap.ActualLRPKey, snap.ActualLRPContainerKey, logger)
}

func (p *lrpProcessor) processReservedContainer(snap *LRPSnapshot, logger lager.Logger) {
	logger = logger.Session("process-reserved-container")
	ok := p.claimSnapshot(logger, snap)
	if !ok {
		return
	}

	ok = p.containerDelegate.RunContainer(logger, snap.Container.Guid)
	if !ok {
		p.bbs.RemoveActualLRP(snap.ActualLRPKey, snap.ActualLRPContainerKey, logger)
		return
	}
}

func (p *lrpProcessor) processInitializingContainer(logger lager.Logger, snap *LRPSnapshot) {
	logger = logger.Session("process-initializing-container")
	p.claimSnapshot(logger, snap)
}

func (p *lrpProcessor) processCreatedContainer(logger lager.Logger, snap *LRPSnapshot) {
	logger = logger.Session("process-created-container")
	p.claimSnapshot(logger, snap)
}

func (p *lrpProcessor) processRunningContainer(logger lager.Logger, snap *LRPSnapshot) {
	logger = logger.Session("process-running-container")
	logger.Debug("extracting-net-info-from-container")
	netInfo, err := rep.ActualLRPNetInfoFromContainer(*snap.Container)
	if err != nil {
		logger.Error("failed-extracting-net-info-from-container", err)
		return
	}
	logger.Debug("succeeded-extracting-net-info-from-container")

	err = p.bbs.StartActualLRP(snap.ActualLRPKey, snap.ActualLRPContainerKey, netInfo, logger)
	if err == bbserrors.ErrActualLRPCannotBeStarted {
		p.containerDelegate.StopContainer(logger, snap.Container.Guid)
	}
}

func (p *lrpProcessor) processCompletedContainer(logger lager.Logger, snap *LRPSnapshot) {
	logger = logger.Session("process-completed-container")
	p.bbs.RemoveActualLRP(snap.ActualLRPKey, snap.ActualLRPContainerKey, logger)
	p.containerDelegate.DeleteContainer(logger, snap.Container.Guid)
}

func (p *lrpProcessor) processInvalidContainer(logger lager.Logger, snap *LRPSnapshot) {
	logger = logger.Session("process-invalid-container")
	logger.Error("not-processing-container-in-invalid-state", nil)
}

func (p *lrpProcessor) claimSnapshot(logger lager.Logger, snap *LRPSnapshot) bool {
	err := p.bbs.ClaimActualLRP(snap.ActualLRPKey, snap.ActualLRPContainerKey, logger)
	switch err {
	case nil:
		return true
	case bbserrors.ErrActualLRPCannotBeClaimed:
		p.containerDelegate.DeleteContainer(logger, snap.Container.Guid)
		return false
	default:
		return false
	}
}

func lagerDataFromLRPSnapshot(snap *LRPSnapshot) lager.Data {
	data := lager.Data{}
	if snap.LRP != nil {
		data["lrp"] = snap.LRP
	}

	if snap.Container != nil {
		data["container-guid"] = snap.Container.Guid
		data["container-state"] = snap.Container.State
	}

	return data
}
