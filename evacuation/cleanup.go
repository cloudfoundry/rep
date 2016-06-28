package evacuation

import (
	"os"

	"code.cloudfoundry.org/bbs"
	"code.cloudfoundry.org/bbs/models"
	"code.cloudfoundry.org/runtimeschema/metric"
	"github.com/pivotal-golang/lager"
)

var strandedEvacuatingActualLRPs = metric.Metric("StrandedEvacuatingActualLRPs")

type EvacuationCleanup struct {
	logger    lager.Logger
	cellID    string
	bbsClient bbs.InternalClient
}

func NewEvacuationCleanup(
	logger lager.Logger,
	cellID string,
	bbsClient bbs.InternalClient,
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

	actualLRPGroups, err := e.bbsClient.ActualLRPGroups(logger, models.ActualLRPFilter{CellID: e.cellID})
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
		err = e.bbsClient.RemoveEvacuatingActualLRP(logger, &actualLRP.ActualLRPKey, &actualLRP.ActualLRPInstanceKey)
		if err != nil {
			logger.Error("failed-removing-evacuating-actual-lrp", err, lager.Data{"lrp-key": actualLRP.ActualLRPKey})
		}
	}

	err = strandedEvacuatingActualLRPs.Send(strandedEvacuationCount)
	if err != nil {
		logger.Error("failed-sending-stranded-evacuating-lrp-metric", err, lager.Data{"count": strandedEvacuationCount})
	}

	return nil
}
