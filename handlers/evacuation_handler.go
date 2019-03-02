package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/locket/metrics/helpers"
	"code.cloudfoundry.org/rep/evacuation/evacuation_context"
)

type evacuationHandler struct {
	evacuatable evacuation_context.Evacuatable
	metrics     helpers.RequestMetrics
}

// Evacuation Handler serves a route that is called by the rep drain script
func newEvacuationHandler(evacuatable evacuation_context.Evacuatable, requestMetrics helpers.RequestMetrics) *evacuationHandler {
	return &evacuationHandler{
		evacuatable: evacuatable,
		metrics:     requestMetrics,
	}
}

func (h *evacuationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request, logger lager.Logger) {
	var deferErr error

	start := time.Now()
	requestType := "Evacuation"
	startMetrics(h.metrics, requestType)
	defer stopMetrics(h.metrics, requestType, time.Since(start), &deferErr)

	logger = logger.Session("handling-evacuation")

	h.evacuatable.Evacuate()

	var jsonBytes []byte
	jsonBytes, deferErr = json.Marshal(map[string]string{"ping_path": "/ping"})
	if deferErr != nil {
		logger.Error("failed-to-marshal-response-payload", deferErr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(jsonBytes)))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write(jsonBytes)
}
