package api

import (
	"net/http"

	"github.com/tedsuo/rata"

	"github.com/cloudfoundry-incubator/rep/routes"
)

func NewServer(lrpHandler http.Handler) (http.Handler, error) {
	handlers := map[string]http.Handler{
		routes.LRPRunning: lrpHandler,
	}

	return rata.NewRouter(routes.Routes, handlers)
}
