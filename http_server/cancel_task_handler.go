package http_server

import (
	"net/http"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/pivotal-golang/lager"
)

type CancelTaskHandler struct {
	logger         lager.Logger
	executorClient executor.Client
}

func NewCancelTaskHandler(logger lager.Logger, executorClient executor.Client) *CancelTaskHandler {
	return &CancelTaskHandler{
		logger:         logger,
		executorClient: executorClient,
	}
}

func (h CancelTaskHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	taskGuid := r.FormValue(":task_guid")

	hLog := h.logger.Session("cancel-task", lager.Data{
		"instance-guid": taskGuid,
	})

	w.WriteHeader(http.StatusAccepted)

	go func() {
		err := h.executorClient.DeleteContainer(taskGuid)
		if err == executor.ErrContainerNotFound {
			hLog.Info("container-not-found")
			return
		}

		if err != nil {
			hLog.Error("failed-to-delete-container", err)
			return
		}

		hLog.Info("stopped")
	}()
}
