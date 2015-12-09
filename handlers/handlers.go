package handlers

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context"
	"github.com/pivotal-golang/lager"
	"github.com/tedsuo/rata"
)

func New(
	localCellClient rep.AuctionCellClient,
	executorClient executor.Client,
	evacuatable evacuation_context.Evacuatable,
	logger lager.Logger,
) rata.Handlers {
	handlers := rata.Handlers{
		rep.StateRoute:     &state{rep: localCellClient, logger: logger},
		rep.PerformRoute:   &perform{rep: localCellClient, logger: logger},
		rep.Sim_ResetRoute: &reset{rep: localCellClient, logger: logger},

		rep.StopLRPInstanceRoute: NewStopLRPInstanceHandler(logger, executorClient),
		rep.CancelTaskRoute:      NewCancelTaskHandler(logger, executorClient),

		rep.PingRoute:     NewPingHandler(),
		rep.EvacuateRoute: NewEvacuationHandler(logger, evacuatable),
	}

	return handlers
}
