package evacuation

import (
	"os"

	"github.com/cloudfoundry-incubator/bbs"
	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/runtime-schema/metric"
	"github.com/pivotal-golang/lager"
)

var strandedEvacuatedActualLRPs = metric.Metric("StrandedEvacuatedActualLRPs")

type EvacuationCleanup struct {
	logger    lager.Logger
	cellID    string
	bbsClient bbs.Client
}

func NewEvacuationCleanup(
	logger lager.Logger,
	cellID string,
	bbsClient bbs.Client,
) *EvacuationCleanup {
	return &EvacuationCleanup{
		logger:    logger,
		cellID:    cellID,
		bbsClient: bbsClient,
	}
}

func (e *EvacuationCleanup) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	logger := e.logger.Session("evacuation-cleanup")

	logger.Info("started")
	defer logger.Info("complete")

	close(ready)

	select {
	case signal := <-signals:
		logger.Info("signalled", lager.Data{"signal": signal})
	}

	actualLRPGroups, err := e.bbsClient.ActualLRPGroups(models.ActualLRPFilter{CellID: e.cellID})
	if err != nil {
		logger.Error("failed-fetching-actual-lrp-groups", err)
		return err
	}

	strandedEvacuationCount := 0
	for _, group := range actualLRPGroups {
		if group.Evacuating == nil {
			continue
		}

		strandedEvacuationCount++
		actualLRP := group.Evacuating
		err = e.bbsClient.RemoveEvacuatingActualLRP(&actualLRP.ActualLRPKey, &actualLRP.ActualLRPInstanceKey)
		if err != nil {
			logger.Error("failed-removing-evacuating-actual-lrp", err, lager.Data{"lrp-key": actualLRP.ActualLRPKey})
		}
	}

	err = strandedEvacuatedActualLRPs.Send(strandedEvacuationCount)
	if err != nil {
		logger.Error("failed-sending-stranded-evaucating-lrp-metric", err, lager.Data{"count": strandedEvacuationCount})
	}

	return nil
}
