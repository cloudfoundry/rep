package harvester

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/bbserrors"
	"github.com/pivotal-golang/lager"
)

type lrpProcessor struct {
	cellID         string
	executorHost   string
	logger         lager.Logger
	bbs            bbs.RepBBS
	executorClient executor.Client
}

func NewLRPProcessor(
	cellID string,
	executorHost string,
	logger lager.Logger,
	bbs bbs.RepBBS,
	executorClient executor.Client,
) Processor {
	return &lrpProcessor{
		cellID:         cellID,
		executorHost:   executorHost,
		logger:         logger,
		bbs:            bbs,
		executorClient: executorClient,
	}
}

func (p *lrpProcessor) Process(container executor.Container) {
	logger := p.logger.Session("process-lrp", lager.Data{
		"container-guid":  container.Guid,
		"container-state": container.State,
	})

	if container.State == executor.StateReserved {
		logger.Debug("ignoring-reserved-container")
		return
	}

	lrpKey, err := rep.ActualLRPKeyFromContainer(container)
	if err != nil {
		logger.Error("failed-to-generate-lrp-key-from-container", err)
		return
	}

	lrpContainerKey, err := rep.ActualLRPContainerKeyFromContainer(container, p.cellID)
	if err != nil {
		logger.Error("failed-to-generate-container-key-from-container", err)
		return
	}

	lrpNetInfo, err := rep.ActualLRPNetInfoFromContainer(container, p.executorHost)
	if err != nil {
		logger.Error("failed-to-generate-net-info-from-container", err)
		return
	}

	switch container.State {
	case executor.StateInitializing, executor.StateCreated:
		_, err = p.bbs.ClaimActualLRP(lrpKey, lrpContainerKey)
		if err == bbserrors.ErrActualLRPCannotBeClaimed {
			logger.Debug("failed-to-claim-actual-lrp")
			p.deleteContainer(lrpContainerKey.InstanceGuid, logger)
			return
		}
		if err != nil {
			logger.Error("failed-to-claim-actual-lrp", err)
			return
		}
		logger.Debug("claimed-actual-lrp")

	case executor.StateRunning:
		_, err := p.bbs.StartActualLRP(lrpKey, lrpContainerKey, lrpNetInfo)
		if err == bbserrors.ErrActualLRPCannotBeStarted {
			logger.Debug("failed-to-start-actual-lrp")
			p.deleteContainer(lrpContainerKey.InstanceGuid, logger)
			return
		}
		if err != nil {
			logger.Error("failed-to-start-actual-lrp", err)
			return
		}
		logger.Debug("started-actual-lrp")

	case executor.StateCompleted:
		err := p.bbs.RemoveActualLRP(lrpKey, lrpContainerKey)
		if err != nil {
			logger.Error("failed-to-remove-actual-lrp", err)
		} else {
			logger.Info("removed-actual-lrp")
		}

		p.deleteContainer(lrpContainerKey.InstanceGuid, logger)
	default:
		logger.Debug("unknown-container-state")
	}
}

func (p *lrpProcessor) deleteContainer(guid string, logger lager.Logger) {
	err := p.executorClient.DeleteContainer(guid)
	if err != nil {
		logger.Error("failed-to-delete-container", err)
	} else {
		logger.Info("deleted-container")
	}
}
