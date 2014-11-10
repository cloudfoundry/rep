package reaper

import (
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/timer"
	"github.com/tedsuo/ifrit"
)

type actualLRPReaper struct {
	pollInterval time.Duration
	timer        timer.Timer

	executorID     string
	bbs            bbs.RepBBS
	executorClient executor.Client
	logger         lager.Logger
}

func NewActualLRPReaper(
	pollInterval time.Duration,
	timer timer.Timer,
	executorID string,
	bbs bbs.RepBBS,
	executorClient executor.Client,
	logger lager.Logger,
) ifrit.Runner {
	return &actualLRPReaper{
		pollInterval:   pollInterval,
		timer:          timer,
		executorID:     executorID,
		bbs:            bbs,
		executorClient: executorClient,
		logger:         logger,
	}
}

func (r *actualLRPReaper) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	close(ready)

	ticks := r.timer.Every(r.pollInterval)

	for {
		select {
		case <-ticks:
			actualLRPs, err := r.bbs.GetAllActualLRPsByExecutorID(r.executorID)
			if err != nil {
				r.logger.Error("failed-to-get-actual-lrps-by-executor-id", err, lager.Data{"executor-id": r.executorID})
				continue
			}

			for _, actualLRP := range actualLRPs {
				_, err = r.executorClient.GetContainer(actualLRP.InstanceGuid)

				if err == executor.ErrContainerNotFound {
					r.logger.Info("found-actual-lrp-with-no-corresponding-container", lager.Data{"actual-lrp": actualLRP})

					err = r.bbs.RemoveActualLRP(actualLRP)
					if err != nil {
						r.logger.Error("failed-to-reap-actual-lrp-with-no-corresponding-container", err, lager.Data{"actual-lrp": actualLRP})
					}
				} else if err != nil {
					r.logger.Error("get-container", err, lager.Data{"actual-lrp": actualLRP})
				}
			}

		case <-signals:
			return nil
		}
	}
}
