package stop_lrp_listener

import (
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor/client"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	steno "github.com/cloudfoundry/gosteno"
)

type StopLRPListener struct {
	lrpStopper lrp_stopper.LRPStopper
	bbs        Bbs.RepBBS
	client     client.Client
	logger     *steno.Logger
}

func New(lrpStopper lrp_stopper.LRPStopper, bbs Bbs.RepBBS, client client.Client, logger *steno.Logger) *StopLRPListener {
	return &StopLRPListener{
		lrpStopper: lrpStopper,
		bbs:        bbs,
		client:     client,
		logger:     logger,
	}
}

func (listener *StopLRPListener) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	stopInstancesChan, stopChan, errChan := listener.bbs.WatchForStopLRPInstance()
	listener.logger.Info("rep.stop-lrp-listener.watching-for-stops")

	close(ready)

	var reWatchChan <-chan time.Time

	for {
		select {
		case stopInstance, ok := <-stopInstancesChan:
			if !ok {
				listener.logger.Error("rep.stop-lrp-listener.watch-closed")
				stopInstancesChan = nil
				break
			}

			listener.logger.Info("rep.stop-lrp-listener.received-stop-lrp-instance")
			go listener.lrpStopper.StopInstance(stopInstance)

		case <-reWatchChan:
			reWatchChan = nil

			stopInstancesChan, stopChan, errChan = listener.bbs.WatchForStopLRPInstance()

		case err := <-errChan:
			listener.logger.Errord(map[string]interface{}{
				"error": err.Error(),
			}, "rep.stop-lrp-listener.received-watch-error")

			stopInstancesChan = nil
			errChan = nil

			reWatchChan = time.After(3 * time.Second)

		case <-signals:
			listener.logger.Info("rep.stop-lrp-listener.shutting-down")
			close(stopChan)
			return nil
		}
	}

	return nil
}
