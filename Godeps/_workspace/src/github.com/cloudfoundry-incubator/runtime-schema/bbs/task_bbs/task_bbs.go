package task_bbs

import (
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/storeadapter"
	"github.com/pivotal-golang/lager"
)

type TaskBBS struct {
	store        storeadapter.StoreAdapter
	timeProvider timeprovider.TimeProvider
	logger       lager.Logger
}

func New(store storeadapter.StoreAdapter, timeProvider timeprovider.TimeProvider, logger lager.Logger) *TaskBBS {
	return &TaskBBS{
		store:        store,
		timeProvider: timeProvider,
		logger:       logger,
	}
}
