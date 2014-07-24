package stop_auction_bbs

import (
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/storeadapter"
	"github.com/pivotal-golang/lager"
)

type StopAuctionBBS struct {
	store        storeadapter.StoreAdapter
	timeProvider timeprovider.TimeProvider
	logger       lager.Logger
}

func New(store storeadapter.StoreAdapter, timeProvider timeprovider.TimeProvider, logger lager.Logger) *StopAuctionBBS {
	return &StopAuctionBBS{
		store:        store,
		timeProvider: timeProvider,
		logger:       logger,
	}
}
