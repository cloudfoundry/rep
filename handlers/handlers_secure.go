package handlers

import (
	"code.cloudfoundry.org/executor"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/rep"
	"code.cloudfoundry.org/rep/evacuation/evacuation_context"
	"github.com/tedsuo/rata"
)

func NewHandlersSecure(
	localCellClient rep.AuctionCellClient,
	executorClient executor.Client,
	evacuatable evacuation_context.Evacuatable,
	logger lager.Logger,
) rata.Handlers {
	stateHandler := &state{rep: localCellClient}
	performHandler := &perform{rep: localCellClient}
	resetHandler := &reset{rep: localCellClient}
	stopLrpHandler := NewStopLRPInstanceHandler(executorClient)
	cancelTaskHandler := NewCancelTaskHandler(executorClient)

	handlers := rata.Handlers{
		rep.StateRoute:     logWrap(stateHandler.ServeHTTP, logger),
		rep.PerformRoute:   logWrap(performHandler.ServeHTTP, logger),
		rep.Sim_ResetRoute: logWrap(resetHandler.ServeHTTP, logger),

		rep.StopLRPInstanceRoute: logWrap(stopLrpHandler.ServeHTTP, logger),
		rep.CancelTaskRoute:      logWrap(cancelTaskHandler.ServeHTTP, logger),
	}

	return handlers
}
