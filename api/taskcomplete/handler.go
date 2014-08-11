package taskcomplete

import (
	"encoding/json"
	"net/http"

	"github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/pivotal-golang/lager"
)

type handler struct {
	bbs            bbs.RepBBS
	executorClient api.Client
	logger         lager.Logger
}

func NewHandler(bbs bbs.RepBBS, executorClient api.Client, logger lager.Logger) http.Handler {
	return &handler{
		bbs:            bbs,
		executorClient: executorClient,
		logger:         logger.Session("complete-handler"),
	}
}

func (handler *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var runResult api.ContainerRunResult
	err := json.NewDecoder(r.Body).Decode(&runResult)
	if err != nil {
		handler.logger.Error("failed-to-unmarshal", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	hLog := handler.logger.Session("complete", lager.Data{
		"guid":   runResult.Guid,
		"failed": runResult.Failed,
		"reason": runResult.FailureReason,
	})

	hLog.Debug("completing")

	err = handler.bbs.CompleteTask(runResult.Guid, runResult.Failed, runResult.FailureReason, runResult.Result)
	if err != nil {
		hLog.Error("failed-to-mark-complete", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = handler.executorClient.DeleteContainer(runResult.Guid)
	if err != nil {
		hLog.Error("failed-to-delete-container", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	hLog.Info("completed")
}
