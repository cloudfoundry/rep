package handlers

import (
	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/pivotal-golang/lager"
	"github.com/tedsuo/rata"
)

func New(localRepClient auctiontypes.CellRep, logger lager.Logger) rata.Handlers {
	handlers := rata.Handlers{
		rep.StateRoute:   &state{rep: localRepClient, logger: logger},
		rep.PerformRoute: &perform{rep: localRepClient, logger: logger},

		rep.Sim_ResetRoute: &reset{rep: localRepClient, logger: logger},
	}

	return handlers
}
