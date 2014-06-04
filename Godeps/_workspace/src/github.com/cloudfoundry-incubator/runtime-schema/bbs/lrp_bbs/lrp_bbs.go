package lrp_bbs

import (
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/storeadapter"
)

type LRPBBS struct {
	store        storeadapter.StoreAdapter
	timeProvider timeprovider.TimeProvider
}

func New(store storeadapter.StoreAdapter, timeProvider timeprovider.TimeProvider) *LRPBBS {
	return &LRPBBS{
		store:        store,
		timeProvider: timeProvider,
	}
}

func (bbs *LRPBBS) DesireLRP(lrp models.DesiredLRP) error {
	return shared.RetryIndefinitelyOnStoreTimeout(func() error {
		return bbs.store.SetMulti([]storeadapter.StoreNode{
			{
				Key:   shared.DesiredLRPSchemaPath(lrp),
				Value: lrp.ToJSON(),
			},
		})
	})
}

func (bbs *LRPBBS) RemoveDesiredLRPByProcessGuid(processGuid string) error {
	return shared.RetryIndefinitelyOnStoreTimeout(func() error {
		err := bbs.store.Delete(shared.DesiredLRPSchemaPathByProcessGuid(processGuid))
		if err == storeadapter.ErrorKeyNotFound {
			return nil
		}
		return err
	})
	return nil
}

func (bbs *LRPBBS) RemoveActualLRP(lrp models.ActualLRP) error {
	return shared.RetryIndefinitelyOnStoreTimeout(func() error {
		return bbs.store.Delete(shared.ActualLRPSchemaPath(lrp))
	})
}

func (bbs *LRPBBS) ReportActualLRPAsStarting(lrp models.ActualLRP) error {
	lrp.State = models.ActualLRPStateStarting
	lrp.Since = bbs.timeProvider.Time().UnixNano()
	return shared.RetryIndefinitelyOnStoreTimeout(func() error {
		return bbs.store.SetMulti([]storeadapter.StoreNode{
			{
				Key:   shared.ActualLRPSchemaPath(lrp),
				Value: lrp.ToJSON(),
			},
		})
	})
}

func (bbs *LRPBBS) ReportActualLRPAsRunning(lrp models.ActualLRP) error {
	lrp.State = models.ActualLRPStateRunning
	lrp.Since = bbs.timeProvider.Time().UnixNano()
	return shared.RetryIndefinitelyOnStoreTimeout(func() error {
		return bbs.store.SetMulti([]storeadapter.StoreNode{
			{
				Key:   shared.ActualLRPSchemaPath(lrp),
				Value: lrp.ToJSON(),
			},
		})
	})
}
