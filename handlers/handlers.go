package handlers

import (
	"net/http"

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
	secure bool,
) rata.Handlers {
	stateHandler := &state{rep: localCellClient}
	performHandler := &perform{rep: localCellClient}
	resetHandler := &reset{rep: localCellClient}
	stopLrpHandler := NewStopLRPInstanceHandler(executorClient)
	cancelTaskHandler := NewCancelTaskHandler(executorClient)
	pingHandler := NewPingHandler()
	evacuationHandler := NewEvacuationHandler(evacuatable)

	handlers := rata.Handlers{
		rep.StateRoute:     logWrap(stateHandler.ServeHTTP, logger),
		rep.PerformRoute:   logWrap(performHandler.ServeHTTP, logger),
		rep.Sim_ResetRoute: logWrap(resetHandler.ServeHTTP, logger),

		rep.StopLRPInstanceRoute: logWrap(stopLrpHandler.ServeHTTP, logger),
		rep.CancelTaskRoute:      logWrap(cancelTaskHandler.ServeHTTP, logger),
	}
	if !secure {
		handlers[rep.PingRoute] = logWrap(pingHandler.ServeHTTP, logger)
		handlers[rep.EvacuateRoute] = logWrap(evacuationHandler.ServeHTTP, logger)
	}

	return handlers
}

func logWrap(loggable func(http.ResponseWriter, *http.Request, lager.Logger), logger lager.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestLog := logger.Session("request", lager.Data{
			"method":  r.Method,
			"request": r.URL.String(),
		})

		defer requestLog.Debug("done")
		requestLog.Debug("serving")

		loggable(w, r, requestLog)
	}
}
