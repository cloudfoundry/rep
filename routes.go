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

func NewRoutes(secure bool) rata.Routes {
	routes := rata.Routes{
		{Path: "/state", Method: "GET", Name: StateRoute},
		{Path: "/work", Method: "POST", Name: PerformRoute},

		{Path: "/v1/lrps/:process_guid/instances/:instance_guid/stop", Method: "POST", Name: StopLRPInstanceRoute},
		{Path: "/v1/tasks/:task_guid/cancel", Method: "POST", Name: CancelTaskRoute},

		{Path: "/sim/reset", Method: "POST", Name: Sim_ResetRoute},
	}

	if !secure {
		routes = append(routes,
			rata.Route{Path: "/ping", Method: "GET", Name: PingRoute},
			rata.Route{Path: "/evacuate", Method: "POST", Name: EvacuateRoute},
		)
	}
	return routes

}

var Routes = NewRoutes(false)
var RoutesSecure = NewRoutes(true)
