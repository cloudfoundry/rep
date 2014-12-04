package harvester

import (
	"errors"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
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
		"container-guid":   container.Guid,
		"container-state":  container.State,
		"container-health": container.Health,
	})

	if container.State == executor.StateReserved {
		logger.Debug("ignoring-reserved-container")
		return
	}

	actualLrp, err := rep.ActualLRPFromContainer(container, p.cellId, p.executorHost)
	if err != nil {
		logger.Error("container-lrp-metdata-validation-failed", err)
		return
	}

	switch container.State {
	case executor.StateInitializing:
		_, err = p.bbs.ReportActualLRPAsStarting(actualLrp.ProcessGuid, actualLrp.InstanceGuid, p.cellId, actualLrp.Domain, actualLrp.Index)
		if err != nil {
			logger.Error("report-starting-failed", err)
			return
		}
		logger.Debug("reported-starting")

	case executor.StateCreated:
		switch container.Health {
		case executor.HealthDown:
			_, err = p.bbs.ReportActualLRPAsStarting(actualLrp.ProcessGuid, actualLrp.InstanceGuid, p.cellId, actualLrp.Domain, actualLrp.Index)
			if err != nil {
				logger.Error("report-starting-failed", err)
				return
			}
			logger.Debug("reported-starting")

		case executor.HealthUp:
			err := p.bbs.ReportActualLRPAsRunning(actualLrp, p.cellId)
			if err != nil {
				logger.Error("report-running-failed", err)
				return
			}
			logger.Debug("reported-running")

		default:
			logger.Error("unknown-container-health", errors.New("inconceivable!"))
		}

	case executor.StateCompleted:
		err := p.bbs.RemoveActualLRP(actualLrp)
		if err != nil {
			logger.Error("remove-actual-lrp-failed", err)
		} else {
			logger.Info("removed-actual-lrp")
		}

		err = p.executorClient.DeleteContainer(actualLrp.InstanceGuid)
		if err != nil {
			logger.Error("delete-actual-lrp-container-failed", err)
		} else {
			logger.Info("delete-actual-lrp-container")
		}

	default:
		logger.Debug("unknown-container-state")
	}
}
