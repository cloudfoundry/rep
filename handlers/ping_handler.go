package handlers

import "net/http"

type PingHandler struct{}

// Ping Handler serves a route that is called by the rep ctl script
func NewPingHandler() *PingHandler {
	return &PingHandler{}
}

func (h PingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
