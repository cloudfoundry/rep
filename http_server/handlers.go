package http_server

import (
	"encoding/json"
	"net/http"

	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/bbserrors"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

type StopLRPInstanceHandler struct {
	stopper lrp_stopper.LRPStopper
}

func NewStopLRPInstanceHandler(stopper lrp_stopper.LRPStopper) *StopLRPInstanceHandler {
	return &StopLRPInstanceHandler{
		stopper: stopper,
	}
}

func (h StopLRPInstanceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var actualLRP models.ActualLRP
	err := json.NewDecoder(r.Body).Decode(&actualLRP)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = actualLRP.Validate()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = h.stopper.StopInstance(actualLRP)
	if err == bbserrors.ErrStoreComparisonFailed {
		w.WriteHeader(http.StatusConflict)
		return
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
