package taskcomplete

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/pivotal-golang/lager"
)

type handler struct {
	bbs            bbs.RepBBS
	executorClient executor.Client
	logger         lager.Logger
}

const MAX_RESULT_SIZE = 1024 * 10

func NewHandler(bbs bbs.RepBBS, executorClient executor.Client, logger lager.Logger) http.Handler {
	return &handler{
		bbs:            bbs,
		executorClient: executorClient,
		logger:         logger.Session("complete-handler"),
	}
}

func (handler *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var runResult executor.ContainerRunResult
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

	guid := runResult.Guid
	failed := runResult.Failed
	reason := runResult.FailureReason
	result := ""
	defer func() {
		err = handler.bbs.CompleteTask(guid, failed, reason, result)
		if err != nil {
			hLog.Error("failed-to-mark-complete", err)
		}

		err = handler.executorClient.DeleteContainer(guid)
		if err != nil {
			hLog.Error("failed-to-delete-container", err)
		} else {
			hLog.Info("completed")
		}
	}()

	if failed {
		return
	}

	task, err := handler.bbs.GetTaskByGuid(guid)
	if err != nil {
		failed = true
		reason = fmt.Sprintf("failed to fetch task: '%s'", err.Error())
		return
	}

	resultFile := task.ResultFile
	if resultFile == "" {
		return
	}

	stream, err := handler.executorClient.GetFiles(guid, resultFile)
	if err != nil {
		failed = true
		reason = fmt.Sprintf("failed to fetch result: '%s'", err.Error())
		return
	}

	tarReader := tar.NewReader(stream)
	_, err = tarReader.Next()
	if err != nil {
		failed = true
		reason = fmt.Sprintf("failed to read contents of the result file: '%s'", err.Error())
		return
	}

	buf := make([]byte, MAX_RESULT_SIZE+1)
	n, err := tarReader.Read(buf)
	if n > MAX_RESULT_SIZE {
		failed = true
		reason = fmt.Sprintf("result file size is %d, max bytes allowed is %d", n, MAX_RESULT_SIZE)
		return
	}

	result = string(buf[:n])
}
