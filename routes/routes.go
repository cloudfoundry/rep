package routes

import "github.com/tedsuo/rata"

const (
	TaskCompleted = "TaskCompleted"
	LRPRunning    = "LRPRunning"
)

var Routes = rata.Routes{
	{Path: "/task_completed/:guid", Method: "PUT", Name: TaskCompleted},
	{Path: "/lrp_running/:process_guid/:index/:instance_guid", Method: "PUT", Name: LRPRunning},
}
