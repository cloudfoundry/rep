package handlers

import (
	"net/http"
	"time"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/locket/metrics/helpers"
)

type pingHandler struct {
	metrics helpers.RequestMetrics
}

// Ping Handler serves a route that is called by the rep ctl script
func newPingHandler(metrics helpers.RequestMetrics) *pingHandler {
	return &pingHandler{
		metrics: metrics,
	}
}

func (h *pingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request, logger lager.Logger) {
	start := time.Now()
	requestType := "Ping"
	startMetrics(h.metrics, requestType)
	defer stopMetrics(h.metrics, requestType, time.Since(start), nil)

	w.WriteHeader(http.StatusOK)
}
