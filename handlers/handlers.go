package handlers

import (
	"code.cloudfoundry.org/executor"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/rep"
	"code.cloudfoundry.org/rep/evacuation/evacuation_context"
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
