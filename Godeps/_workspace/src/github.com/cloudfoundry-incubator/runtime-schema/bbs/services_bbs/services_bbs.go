package services_bbs

import (
	"github.com/cloudfoundry/storeadapter"
	"github.com/pivotal-golang/lager"
)

type ServicesBBS struct {
	store  storeadapter.StoreAdapter
	logger lager.Logger
}

func New(store storeadapter.StoreAdapter, logger lager.Logger) *ServicesBBS {
	return &ServicesBBS{
		store:  store,
		logger: logger,
	}
}
