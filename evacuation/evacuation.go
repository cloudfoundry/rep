package evacuation

import (
	"os"
	"time"

	"github.com/cloudfoundry-incubator/bbs"
	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context"
	"github.com/cloudfoundry-incubator/runtime-schema/metric"
	"github.com/pivotal-golang/clock"
	"github.com/pivotal-golang/lager"
)

var (
	strandedEvacuatedActualLRPMetric = metric.Metric("StrandedEvacuatedActualLRPs")
)

type Evacuator struct {
	bbsClient          bbs.Client
	logger             lager.Logger
	clock              clock.Clock
	executorClient     executor.Client
	evacuationNotifier evacuation_context.EvacuationNotifier
	cellID             string
	evacuationTimeout  time.Duration
	pollingInterval    time.Duration
}

func NewEvacuator(
	bbsClient bbs.Client,
	logger lager.Logger,
	clock clock.Clock,
	executorClient executor.Client,
	evacuationNotifier evacuation_context.EvacuationNotifier,
	cellID string,
	evacuationTimeout time.Duration,
	pollingInterval time.Duration,
) *Evacuator {
	return &Evacuator{
		bbsClient:          bbsClient,
		logger:             logger,
		clock:              clock,
		executorClient:     executorClient,
		evacuationNotifier: evacuationNotifier,
		cellID:             cellID,
		evacuationTimeout:  evacuationTimeout,
		pollingInterval:    pollingInterval,
	}
}

func (e *Evacuator) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	logger := e.logger.Session("running-evacuator")
	logger.Info("started")
	defer logger.Info("finished")

	evacuationNotify := e.evacuationNotifier.EvacuateNotify()
	close(ready)

	select {
	case signal := <-signals:
		logger.Info("signaled", lager.Data{"signal": signal.String()})
		return nil
	case <-evacuationNotify:
		evacuationNotify = nil
		logger.Info("notified-of-evacuation")
	}

	timer := e.clock.NewTimer(e.evacuationTimeout)
	defer timer.Stop()

	doneCh := make(chan struct{})
	go e.evacuate(logger, doneCh)

	select {
	case <-doneCh:
		logger.Info("evacuation-complete")
		e.removeEvacuatingActualLRPs(logger)
		return nil
	case <-timer.C():
		logger.Error("failed-to-evacuate-before-timeout", nil)
		e.removeEvacuatingActualLRPs(logger)
		return nil
	case signal := <-signals:
		logger.Info("signaled", lager.Data{"signal": signal.String()})
		return nil
	}

	return nil
}

func (e *Evacuator) removeEvacuatingActualLRPs(logger lager.Logger) {
	logger = logger.Session("remove-evacuating-lrps")
	logger.Info("started")
	defer logger.Info("ended")

	actualLRPGroups, err := e.bbsClient.ActualLRPGroups(models.ActualLRPFilter{CellID: e.cellID})
	if err != nil {
		e.logger.Error("failed-getting-actual-lrps", err)
		return
	}

	count := 0
	for _, actualLRPGroup := range actualLRPGroups {
		actualLRP, evacuating := actualLRPGroup.Resolve()
		if evacuating {
			count++
			err = e.bbsClient.RemoveEvacuatingActualLRP(&actualLRP.ActualLRPKey, &actualLRP.ActualLRPInstanceKey)
		}
	}
	err = strandedEvacuatedActualLRPMetric.Send(count)
	if err != nil {
		logger.Error("failed-to-send-stranded-evacuated-lrps-metric", err)
	}
}

func (e *Evacuator) evacuate(logger lager.Logger, doneCh chan<- struct{}) {
	logger = logger.Session("evacuating")
	logger.Info("started")

	timer := e.clock.NewTimer(e.pollingInterval)
	defer timer.Stop()

	for {
		evacuated := e.allContainersEvacuated(logger)

		if !evacuated {
			logger.Info("evacuation-incomplete", lager.Data{"polling-interval": e.pollingInterval})
			timer.Reset(e.pollingInterval)
			<-timer.C()
			continue
		}

		close(doneCh)
		logger.Info("succeeded")

		return
	}
}

func (e *Evacuator) allContainersEvacuated(logger lager.Logger) bool {
	containers, err := e.executorClient.ListContainers(logger)
	if err != nil {
		logger.Error("failed-to-list-containers", err)
		return false
	}

	return len(containers) == 0
}
