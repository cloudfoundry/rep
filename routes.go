package rep

import "github.com/tedsuo/rata"

const (
	StateRoute   = "STATE"
	PerformRoute = "PERFORM"

	StopLRPInstanceRoute = "StopLRPInstance"
	CancelTaskRoute      = "CancelTask"

	Sim_ResetRoute = "RESET"

	PingRoute     = "Ping"
	EvacuateRoute = "Evacuate"
)

var Routes = rata.Routes{
	{Path: "/state", Method: "GET", Name: StateRoute},
	{Path: "/work", Method: "POST", Name: PerformRoute},

	{Path: "/v1/lrps/:process_guid/instances/:instance_guid/stop", Method: "POST", Name: StopLRPInstanceRoute},
	{Path: "/v1/tasks/:task_guid/cancel", Method: "POST", Name: CancelTaskRoute},

	{Path: "/sim/reset", Method: "POST", Name: Sim_ResetRoute},

	// These routes are called by the rep ctl and drain scripts
	{Path: "/ping", Method: "GET", Name: PingRoute},
	{Path: "/evacuate", Method: "POST", Name: EvacuateRoute},
}
