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

	lrps := snapshot.ActualLRPs()

	for _, lrp := range lrps {
		_, ok := snapshot.GetContainer(lrp.InstanceGuid)

		if !ok {
			r.logger.Info("actual-lrp-with-no-corresponding-container", lager.Data{"actual-lrp": lrp})

			err := r.bbs.RemoveActualLRP(lrp.ActualLRPKey, lrp.ActualLRPContainerKey)
			if err != nil {
				r.logger.Error("failed-to-reap-actual-lrp-with-no-corresponding-container", err, lager.Data{"actual-lrp": lrp})
			}
		}
	}

	r.logger.Info("stopped")
}
