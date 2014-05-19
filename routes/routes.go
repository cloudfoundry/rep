package routes

import "github.com/tedsuo/router"

const (
	TaskCompleted = "TaskCompleted"
	LRPCompleted  = "LRPCompleted"

	RouteHealthy   = "RouteHealthy"
	RouteUnhealthy = "RouteUnhealthy"
)

var Routes = router.Routes{
	{Path: "/task_completed/:guid", Method: "POST", Handler: TaskCompleted},
	{Path: "/lrp_completed/:guid", Method: "POST", Handler: LRPCompleted},

	//{Path: "/routes/:guid/healthy", Method: "PUT", Handler: RouteHealthy},
	//{Path: "/routes/:guid/unhealthy", Method: "PUT", Handler: RouteUnhealthy},
}
