package lrp_stopper

import (
	executorapi "github.com/cloudfoundry-incubator/executor/api"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	steno "github.com/cloudfoundry/gosteno"
)

type LRPStopper interface {
	StopInstance(stopInstance models.StopLRPInstance) error
}

type lrpStopper struct {
	bbs    Bbs.RepBBS
	client executorapi.Client
	logger *steno.Logger
}

func New(bbs Bbs.RepBBS, client executorapi.Client, logger *steno.Logger) LRPStopper {
	return &lrpStopper{
		bbs:    bbs,
		client: client,
		logger: logger,
	}
}

func (stopper *lrpStopper) StopInstance(stopInstance models.StopLRPInstance) error {
	stopper.logger.Debugd(map[string]interface{}{
		"stop-instance": stopInstance,
	}, "rep.lrp-stopper.received-stop-instance")

	containerId := stopInstance.LRPIdentifier().OpaqueID()
	_, err := stopper.client.GetContainer(containerId)
	if err != nil {
		return err
	}

	stopper.logger.Infod(map[string]interface{}{
		"stop-instance": stopInstance,
	}, "rep.lrp-stopper.stopping-instance")

	err = stopper.bbs.ResolveStopLRPInstance(stopInstance)
	if err != nil {
		stopper.logger.Infod(map[string]interface{}{
			"stop-instance": stopInstance,
			"error":         err.Error(),
		}, "rep.lrp-stopper.resolve-stop-lrp-failed")
		return err
	}

	err = stopper.client.DeleteContainer(containerId)
	if err != nil {
		stopper.logger.Infod(map[string]interface{}{
			"stop-instance": stopInstance,
			"error":         err.Error(),
		}, "rep.lrp-stopper.delete-container-failed")
	}

	return err
}
