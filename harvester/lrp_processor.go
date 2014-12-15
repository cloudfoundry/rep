package harvester

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/bbserrors"
	"github.com/pivotal-golang/lager"
)

type lrpProcessor struct {
	cellId         string
	executorHost   string
	logger         lager.Logger
	bbs            bbs.RepBBS
	executorClient executor.Client
}

func NewLRPProcessor(
	cellId string,
	executorHost string,
	logger lager.Logger,
	bbs bbs.RepBBS,
	executorClient executor.Client,
) Processor {
	return &lrpProcessor{
		cellId:         cellId,
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

	lrpKey, lrpContainerKey, lrpNetInfo, err := rep.ActualLRPFromContainer(container, p.cellId, p.executorHost)
	if err != nil {
		logger.Error("failed-to-validate-container-lrp-metdata", err)
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
