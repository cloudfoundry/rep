package lrp_stopper

import (
	"os"
	"syscall"

	"github.com/cloudfoundry-incubator/executor/client"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	steno "github.com/cloudfoundry/gosteno"
)

type LRPStopper struct {
	bbs    Bbs.RepBBS
	client client.Client
	logger *steno.Logger
}

func New(bbs Bbs.RepBBS, client client.Client, logger *steno.Logger) *LRPStopper {
	return &LRPStopper{
		bbs:    bbs,
		client: client,
		logger: logger,
	}
}

func (stopper *LRPStopper) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	stopInstancesChan, stopChan, errChan := stopper.bbs.WatchForStopLRPInstance()

	if ready != nil {
		close(ready)
	}

	for {
		if stopInstancesChan == nil {
			stopInstancesChan, stopChan, errChan = stopper.bbs.WatchForStopLRPInstance()
		}
		select {
		case stopInstance, ok := <-stopInstancesChan:
			if !ok {
				stopInstancesChan = nil
				break
			}
			go stopper.handleStopInstance(stopInstance)

		case sig := <-signals:
			switch sig {
			case syscall.SIGTERM, syscall.SIGINT:
				close(stopChan)
				return nil
			}

		case _ = <-errChan:
			stopInstancesChan = nil
		}
	}

	return nil
}

func (stopper *LRPStopper) handleStopInstance(stopInstance models.StopLRPInstance) {
	_, err := stopper.client.GetContainer(stopInstance.InstanceGuid)
	if err != nil {
		return
	}
	stopper.client.DeleteContainer(stopInstance.InstanceGuid)

	stopper.bbs.ResolveStopLRPInstance(stopInstance)

}
