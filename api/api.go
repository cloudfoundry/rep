package api

import (
	"net/http"

	"github.com/tedsuo/router"

	"github.com/cloudfoundry-incubator/rep/routes"
)

func NewServer(taskHandler, lrpHandler http.Handler) (http.Handler, error) {
	handlers := map[string]http.Handler{
		routes.TaskCompleted: taskHandler,
		routes.LRPCompleted:  lrpHandler,

		routes.RouteHealthy:   nil,
		routes.RouteUnhealthy: nil,
	}

	return router.NewRouter(routes.Routes, handlers)
}
