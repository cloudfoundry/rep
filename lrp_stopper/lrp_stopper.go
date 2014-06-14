package lrp_stopper

import (
	"github.com/cloudfoundry-incubator/executor/client"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	steno "github.com/cloudfoundry/gosteno"
)

type LRPStopper interface {
	StopInstance(stopInstance models.StopLRPInstance) error
}

type lrpStopper struct {
	bbs    Bbs.RepBBS
	client client.Client
	logger *steno.Logger
}

func New(bbs Bbs.RepBBS, client client.Client, logger *steno.Logger) LRPStopper {
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

	_, err := stopper.client.GetContainer(stopInstance.InstanceGuid)
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

	err = stopper.client.DeleteContainer(stopInstance.InstanceGuid)
	if err != nil {
		stopper.logger.Infod(map[string]interface{}{
			"stop-instance": stopInstance,
			"error":         err.Error(),
		}, "rep.lrp-stopper.delete-container-failed")
	}

	return err
}
