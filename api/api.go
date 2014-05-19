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
	}

	return router.NewRouter(routes.Routes, handlers)
}
