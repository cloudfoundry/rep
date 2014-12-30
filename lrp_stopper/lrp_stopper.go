package lrp_stopper

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/pivotal-golang/lager"
)

//go:generate counterfeiter -o fake_lrp_stopper/fake_lrpstopper.go . LRPStopper
type LRPStopper interface {
	StopInstance(processGuid, instanceGuid string) error
}

type lrpStopper struct {
	guid   string
	client executor.Client
	logger lager.Logger
}

func New(guid string, client executor.Client, logger lager.Logger) LRPStopper {
	return &lrpStopper{
		guid:   guid,
		client: client,
		logger: logger.Session("lrp-stopper"),
	}
}

func (stopper *lrpStopper) StopInstance(processGuid, instanceGuid string) error {
	stopLog := stopper.logger.Session("stop", lager.Data{
		"process-guid":  processGuid,
		"instance-guid": instanceGuid,
	})

	stopLog.Info("stopping")
	defer stopLog.Info("stopped")

	return stopper.client.StopContainer(instanceGuid)
}
