package api

import (
	"net/http"

	"github.com/tedsuo/rata"

	"github.com/cloudfoundry-incubator/rep/routes"
)

func NewServer(taskHandler, lrpHandler http.Handler) (http.Handler, error) {
	handlers := map[string]http.Handler{
		routes.TaskCompleted: taskHandler,
		routes.LRPRunning:    lrpHandler,
	}

	return rata.NewRouter(routes.Routes, handlers)
}
