package start_auction_bbs

import (
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/storeadapter"
	"github.com/pivotal-golang/lager"
)

type StartAuctionBBS struct {
	store        storeadapter.StoreAdapter
	timeProvider timeprovider.TimeProvider
	logger       lager.Logger
}

func New(store storeadapter.StoreAdapter, timeProvider timeprovider.TimeProvider, logger lager.Logger) *StartAuctionBBS {
	return &StartAuctionBBS{
		store:        store,
		timeProvider: timeProvider,
		logger:       logger,
	}
}
