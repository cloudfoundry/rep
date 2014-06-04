package lrp_stopper

import (
	"os"
	"time"

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
	stopper.logger.Info("rep.lrp-stopper.watching-for-stops")

	if ready != nil {
		close(ready)
	}

	for {
		if stopInstancesChan == nil {
			time.Sleep(3 * time.Second)
			stopInstancesChan, stopChan, errChan = stopper.bbs.WatchForStopLRPInstance()
		}

		select {
		case stopInstance, ok := <-stopInstancesChan:
			if ok {
				go stopper.handleStopInstance(stopInstance)
			} else {
				stopper.logger.Error("rep.lrp-stopper.watch-closed")
				stopInstancesChan = nil
				break
			}

		case <-signals:
			stopper.logger.Info("rep.lrp-stopper.shutting-down")
			close(stopChan)
			return nil

		case err, ok := <-errChan:
			if ok {
				stopper.logger.Errord(map[string]interface{}{
					"error": err.Error(),
				}, "rep.lrp-stopper.received-watch-error")
			}
			stopInstancesChan = nil

		}
	}

	return nil
}

func (stopper *LRPStopper) handleStopInstance(stopInstance models.StopLRPInstance) {
	stopper.logger.Debugd(map[string]interface{}{
		"stop-instance": stopInstance,
	}, "rep.lrp-stopper.received-stop-instance")

	_, err := stopper.client.GetContainer(stopInstance.InstanceGuid)
	if err != nil {
		return
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
		return
	}

	err = stopper.client.DeleteContainer(stopInstance.InstanceGuid)
	if err != nil {
		stopper.logger.Infod(map[string]interface{}{
			"stop-instance": stopInstance,
			"error":         err.Error(),
		}, "rep.lrp-stopper.delete-container-failed")
	}
}
