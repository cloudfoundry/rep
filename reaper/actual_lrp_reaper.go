package reaper

import (
	"github.com/cloudfoundry-incubator/rep/gatherer"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/pivotal-golang/lager"
)

type ActualLRPReaper struct {
	bbs    bbs.RepBBS
	logger lager.Logger
}

func NewActualLRPReaper(
	bbs bbs.RepBBS,
	logger lager.Logger,
) *ActualLRPReaper {
	return &ActualLRPReaper{
		bbs:    bbs,
		logger: logger.Session("actual-lrp-reaper"),
	}
}

func (r *ActualLRPReaper) Process(snapshot gatherer.Snapshot) {
	r.logger.Info("started")

	actualLRPs := snapshot.ActualLRPs()

	for _, actualLRP := range actualLRPs {
		container := snapshot.GetContainer(actualLRP.InstanceGuid)

		if container == nil {
			r.logger.Info("actual-lrp-with-no-corresponding-container", lager.Data{"actual-lrp": actualLRP})

			err := r.bbs.RemoveActualLRP(actualLRP)
			if err != nil {
				r.logger.Error("failed-to-reap-actual-lrp-with-no-corresponding-container", err, lager.Data{"actual-lrp": actualLRP})
			}
		}
	}

	r.logger.Info("stopped")
}
