package http_server

import (
	"net/http"
	"strconv"

	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
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
	index, err := strconv.Atoi(r.FormValue(":index"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	stopInstance := models.StopLRPInstance{
		ProcessGuid:  r.FormValue(":process_guid"),
		InstanceGuid: r.FormValue(":instance_guid"),
		Index:        index,
	}
	err = stopInstance.Validate()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	h.stopper.StopInstance(stopInstance)
	w.WriteHeader(http.StatusAccepted)
}
