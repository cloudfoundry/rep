package lrp_stopper

import (
	"github.com/cloudfoundry-incubator/executor"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

type LRPStopper interface {
	StopInstance(models.ActualLRP) error
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

func (stopper *lrpStopper) StopInstance(lrp models.ActualLRP) error {
	stopLog := stopper.logger.Session("stop", lager.Data{
		"lrp": lrp,
	})

	stopLog.Info("received")

	containerId := lrp.InstanceGuid

	stopLog.Info("stopping", lager.Data{
		"container": containerId,
	})

	err := stopper.client.DeleteContainer(containerId)
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

	err = stopper.bbs.RemoveActualLRP(lrp)
	if err != nil {
		stopLog.Error("failed-to-remove-actual-lrp", err)
		return err
	}

	return nil
}
