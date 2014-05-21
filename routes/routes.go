package routes

import "github.com/tedsuo/router"

const (
	TaskCompleted = "TaskCompleted"
	LRPRunning    = "LRPRunning"
)

var Routes = router.Routes{
	{Path: "/task_completed/:guid", Method: "PUT", Handler: TaskCompleted},
	{Path: "/lrp_running/:process_guid/:index/:instance_guid", Method: "PUT", Handler: LRPRunning},
}
