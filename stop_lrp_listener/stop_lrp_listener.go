package stop_lrp_listener

import (
	"os"
	"time"

	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/pivotal-golang/lager"
)

type StopLRPListener struct {
	lrpStopper lrp_stopper.LRPStopper
	bbs        Bbs.RepBBS
	logger     lager.Logger
}

func New(lrpStopper lrp_stopper.LRPStopper, bbs Bbs.RepBBS, logger lager.Logger) *StopLRPListener {
	return &StopLRPListener{
		lrpStopper: lrpStopper,
		bbs:        bbs,
		logger:     logger.Session("stop-lrp-listener"),
	}
}

func (listener *StopLRPListener) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	stopInstancesChan, stopChan, errChan := listener.bbs.WatchForStopLRPInstance()

	listener.logger.Info("watching")

	close(ready)

	var reWatchChan <-chan time.Time

	for {
		select {
		case stopInstance, ok := <-stopInstancesChan:
			if !ok {
				listener.logger.Error("watch-closed", nil)
				stopInstancesChan = nil
				break
			}

			listener.logger.Info("received-stop", lager.Data{
				"instance": stopInstance,
			})

			go listener.lrpStopper.StopInstance(stopInstance)

		case <-reWatchChan:
			reWatchChan = nil

			stopInstancesChan, stopChan, errChan = listener.bbs.WatchForStopLRPInstance()

		case err := <-errChan:
			listener.logger.Error("watch-error", err)

			stopInstancesChan = nil
			errChan = nil

			reWatchChan = time.After(3 * time.Second)

		case <-signals:
			listener.logger.Info("shutting-down")
			close(stopChan)
			return nil
		}
	}

	return nil
}
