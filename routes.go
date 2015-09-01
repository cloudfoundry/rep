package rep

import "github.com/tedsuo/rata"

const (
	StateRoute   = "STATE"
	PerformRoute = "PERFORM"

	Sim_ResetRoute = "RESET"
)

var Routes = rata.Routes{
	{Path: "/state", Method: "GET", Name: StateRoute},
	{Path: "/work", Method: "POST", Name: PerformRoute},

	{Path: "/sim/reset", Method: "POST", Name: Sim_ResetRoute},
}
