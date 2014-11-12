package harvester

import (
	"errors"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/pivotal-golang/lager"
)

type lrpProcessor struct {
	executorId     string
	executorHost   string
	logger         lager.Logger
	bbs            bbs.RepBBS
	executorClient executor.Client
}

func NewLRPProcessor(
	executorId string,
	executorHost string,
	logger lager.Logger,
	bbs bbs.RepBBS,
	executorClient executor.Client,
) Processor {
	return &lrpProcessor{
		executorId:     executorId,
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

	actualLrp, err := rep.ActualLRPFromContainer(container, p.executorId, p.executorHost)
	if err != nil {
		logger.Error("container-lrp-metdata-validation-failed", err)
		return
	}

	switch container.State {
	case executor.StateInitializing:
		_, err = p.bbs.ReportActualLRPAsStarting(actualLrp.ProcessGuid, actualLrp.InstanceGuid, p.executorId, actualLrp.Domain, actualLrp.Index)
		if err != nil {
			logger.Error("report-starting-failed", err)
			return
		}
		logger.Debug("reported-starting")

	case executor.StateCreated:
		switch container.Health {
		case executor.HealthDown:
			_, err = p.bbs.ReportActualLRPAsStarting(actualLrp.ProcessGuid, actualLrp.InstanceGuid, p.executorId, actualLrp.Domain, actualLrp.Index)
			if err != nil {
				logger.Error("report-starting-failed", err)
				return
			}
			logger.Debug("reported-starting")

		case executor.HealthUp, executor.HealthUnmonitored:
			err := p.bbs.ReportActualLRPAsRunning(actualLrp, p.executorId)
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
			return
		}
		logger.Info("removed-actual-lrp")

	default:
		logger.Debug("unknown-container-state")
	}
}
