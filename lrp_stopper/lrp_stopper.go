package lrp_stopper

import (
	executorapi "github.com/cloudfoundry-incubator/executor/api"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

type LRPStopper interface {
	StopInstance(stopInstance models.StopLRPInstance) error
}

type lrpStopper struct {
	bbs    Bbs.RepBBS
	client executorapi.Client
	logger lager.Logger
}

func New(bbs Bbs.RepBBS, client executorapi.Client, logger lager.Logger) LRPStopper {
	return &lrpStopper{
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

	containerId := stopInstance.LRPIdentifier().OpaqueID()

	_, err := stopper.client.GetContainer(containerId)
	if err != nil {
		return err
	}

	stopLog.Info("stopping", lager.Data{
		"container": containerId,
	})

	err = stopper.bbs.ResolveStopLRPInstance(stopInstance)
	if err != nil {
		stopLog.Error("failed-to-resolve-stop", err)
		return err
	}

	err = stopper.client.DeleteContainer(containerId)
	if err != nil {
		stopLog.Error("failed-to-delete-container", err)
		return err
	}

	return nil
}
