package handlers

import (
	"net/http"

	"github.com/pivotal-golang/lager"
)

type StopContainerHandler struct {
	logger         lager.Logger
	executorClient executor.Client
}

func NewStopContainerHandler(logger lager.Logger, executorClient executor.Client) *StopContainerHandler {
	return &StopContainerHandler{
		logger:         logger,
		executorClient: executorClient,
	}
}

func (h *StopContainerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	containerGuid := r.FormValue(":container_guid")

	logger := h.logger.Session("stop-container", lager.Data{
		"container-guid": containerGuid,
	})

	w.WriteHeader(http.StatusAccepted)

	go func() {
		logger.Info("deleting-container")
		err := h.executorClient.StopContainer(containerGuid)
		switch err {
		case nil:
			logger.Info("succeeded-deleting-container")
		case executor.ErrContainerNotFound:
			logger.Info("container-not-found")
		default:
			logger.Error("failed-deleting-container", err)
		}
	}()
}
