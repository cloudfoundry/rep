package taskcomplete

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry/gosteno"
)

type handler struct {
	bbs            bbs.RepBBS
	executorClient api.Client
	logger         *gosteno.Logger
}

func NewHandler(bbs bbs.RepBBS, executorClient api.Client, logger *gosteno.Logger) http.Handler {
	return &handler{
		bbs:            bbs,
		executorClient: executorClient,
		logger:         logger,
	}
}

func (handler *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var runResult api.ContainerRunResult
	err := json.NewDecoder(r.Body).Decode(&runResult)
	if err != nil {
		handler.logger.Errord(map[string]interface{}{
			"error": fmt.Sprintf("Could not unmarshal response: %s", err),
		}, "game-scheduler.complete-callback-handler.failed")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	handler.bbs.CompleteTask(runResult.Guid, runResult.Failed, runResult.FailureReason, runResult.Result)
	handler.executorClient.DeleteContainer(runResult.Guid)
}
