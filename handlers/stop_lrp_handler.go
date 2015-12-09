package handlers

import (
	"errors"
	"net/http"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/pivotal-golang/lager"
)

type StopLRPInstanceHandler struct {
	logger lager.Logger
	client executor.Client
}

func NewStopLRPInstanceHandler(logger lager.Logger, client executor.Client) *StopLRPInstanceHandler {
	return &StopLRPInstanceHandler{
		logger: logger,
		client: client,
	}
}

func (h StopLRPInstanceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	processGuid := r.FormValue(":process_guid")
	instanceGuid := r.FormValue(":instance_guid")

	logger := h.logger.Session("handling-stop-lrp-instance", lager.Data{
		"process-guid":  processGuid,
		"instance-guid": instanceGuid,
	})

	if processGuid == "" {
		err := errors.New("process_guid missing from request")
		logger.Error("missing-process-guid", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if instanceGuid == "" {
		err := errors.New("instance_guid missing from request")
		logger.Error("missing-instance-guid", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusAccepted)

	go h.client.StopContainer(logger, rep.LRPContainerGuid(processGuid, instanceGuid))
}
