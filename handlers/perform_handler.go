package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"code.cloudfoundry.org/lager/v3"
	"code.cloudfoundry.org/locket/metrics/helpers"
	"code.cloudfoundry.org/rep"
	"code.cloudfoundry.org/rep/auctioncellrep"
)

type perform struct {
	rep     auctioncellrep.AuctionCellClient
	metrics helpers.RequestMetrics
}

func newPerformHandler(rep auctioncellrep.AuctionCellClient, metrics helpers.RequestMetrics) *perform {
	return &perform{rep: rep, metrics: metrics}
}

func (h *perform) ServeHTTP(w http.ResponseWriter, r *http.Request, logger lager.Logger) {
	var deferErr error

	start := time.Now()
	requestType := "Perform"
	startMetrics(h.metrics, requestType)
	defer stopMetrics(h.metrics, requestType, start, &deferErr)

	logger = logger.Session("auction-perform-work")
	var work rep.Work
	deferErr = json.NewDecoder(r.Body).Decode(&work)
	if deferErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		logger.Error("failed-to-unmarshal", deferErr)
		return
	}

	var failedWork rep.Work
	failedWork, deferErr = h.rep.Perform(logger, work)
	if deferErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		logger.Error("failed-to-perform-work", deferErr)
		return
	}

	json.NewEncoder(w).Encode(failedWork)
}
