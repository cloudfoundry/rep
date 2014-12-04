package lrp_stopper

import (
	"github.com/cloudfoundry-incubator/executor"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

type LRPStopper interface {
	StopInstance(stopInstance models.StopLRPInstance) error
}

type lrpStopper struct {
	guid   string
	bbs    Bbs.RepBBS
	client executor.Client
	logger lager.Logger
}

func New(guid string, bbs Bbs.RepBBS, client executor.Client, logger lager.Logger) LRPStopper {
	return &lrpStopper{
		guid:   guid,
		bbs:    bbs,
		client: client,
		logger: logger.Session("lrp-stopper"),
	}
}

func (stopper *lrpStopper) StopInstance(stopInstance models.StopLRPInstance) error {
	stopLog := stopper.logger.Session("stop", lager.Data{
		"stop-instance": stopInstance,
	})

	stopLog.Info("received")

	containerId := stopInstance.InstanceGuid

	isResponsible, err := stopper.isResponsible(stopInstance.ProcessGuid, stopInstance.InstanceGuid)
	if err != nil {
		stopLog.Error("failed-to-fetch-actual-lrps", err)
		return err
	}

	if !isResponsible {
		return nil
	}

	stopLog.Info("stopping", lager.Data{
		"container": containerId,
	})

	err = stopper.client.DeleteContainer(containerId)
	switch err {
	case nil:
	case executor.ErrContainerNotFound:
		stopLog.Info("container-already-gon", lager.Data{
			"container-id": containerId,
		})
	default:
		stopLog.Error("failed-to-delete-container", err, lager.Data{
			"container-id": containerId,
		})
		return err
	}

	err = stopper.bbs.RemoveActualLRPForIndex(stopInstance.ProcessGuid, stopInstance.Index, stopInstance.InstanceGuid)
	if err != nil {
		stopLog.Error("failed-to-remove-actual-lrp", err, lager.Data{
			"process-guid":       stopInstance.ProcessGuid,
			"stopInstance-index": stopInstance.Index,
			"stopInstance-guid":  stopInstance.InstanceGuid,
		})
		return err
	}

	return nil
}

func (stopper *lrpStopper) isResponsible(processGuid, instanceGuid string) (bool, error) {
	actuals, err := stopper.bbs.ActualLRPsByProcessGuid(processGuid)
	if err != nil {
		return false, err
	}

	for _, actual := range actuals {
		if actual.InstanceGuid == instanceGuid && actual.CellID == stopper.guid {
			return true, nil
		}
	}

	return false, nil
}
