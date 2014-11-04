package routes

import "github.com/tedsuo/rata"

const (
	LRPRunning = "LRPRunning"
)

var Routes = rata.Routes{
	{Path: "/lrp_running/:process_guid/:index/:instance_guid", Method: "PUT", Name: LRPRunning},
}
