package http_server

import (
	"encoding/json"
	"net/http"

	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/bbserrors"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

type StopLRPInstanceHandler struct {
	logger  lager.Logger
	stopper lrp_stopper.LRPStopper
}

func NewStopLRPInstanceHandler(logger lager.Logger, stopper lrp_stopper.LRPStopper) *StopLRPInstanceHandler {
	return &StopLRPInstanceHandler{
		logger:  logger,
		stopper: stopper,
	}
}

func (h StopLRPInstanceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	hLog := h.logger.Session("stop-lrp-instance")

	var actualLRP models.ActualLRP
	err := json.NewDecoder(r.Body).Decode(&actualLRP)
	if err != nil {
		hLog.Error("malformed-request", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	hLog = h.logger.WithData(lager.Data{
		"process-guid":  actualLRP.ProcessGuid,
		"instance-guid": actualLRP.InstanceGuid,
		"index":         actualLRP.Index,
	})

	err = actualLRP.Validate()
	if err != nil {
		hLog.Error("invalid-lrp", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusAccepted)

	go func() {
		err = h.stopper.StopInstance(actualLRP)
		if err == bbserrors.ErrStoreComparisonFailed {
			return
		}

		if err != nil {
			h.logger.Error("failed-to-stop", err)
			return
		}

		h.logger.Info("stopped")
	}()
}
