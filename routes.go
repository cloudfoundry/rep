package rep

import "github.com/tedsuo/rata"

const (
	StopLRPInstanceRoute = "StopLRPInstance"

	CancelTaskRoute = "CancelTask"
)

var CellRoutes = rata.Routes{
	{Name: StopLRPInstanceRoute, Method: "POST", Path: "/lrps/:process_guid/instances/:instance_guid/stop"},

	{Name: CancelTaskRoute, Method: "POST", Path: "/tasks/:task_guid/cancel"},
}
