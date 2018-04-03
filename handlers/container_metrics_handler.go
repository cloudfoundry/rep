package handlers

import (
	"encoding/json"
	"net/http"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/rep"
)

//go:generate counterfeiter . MetricCollector
type MetricCollector interface {
	Metrics(logger lager.Logger) (*rep.ContainerMetricsCollection, error)
}

type containerMetrics struct {
	rep MetricCollector
}

func (h *containerMetrics) ServeHTTP(w http.ResponseWriter, r *http.Request, logger lager.Logger) {
	logger = logger.Session("container-metrics-handler")

	m, err := h.rep.Metrics(logger)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		logger.Error("failed-to-fetch-container-metrics", err)
		return
	}

	json.NewEncoder(w).Encode(m)
}
