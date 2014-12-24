package harvester

import (
	"archive/tar"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/pivotal-golang/lager"
)

const MAX_RESULT_SIZE = 1024 * 10

type taskProcessor struct {
	logger lager.Logger

	cellID         string
	bbs            bbs.RepBBS
	executorClient executor.Client
}

func NewTaskProcessor(
	logger lager.Logger,
	cellID string,
	bbs bbs.RepBBS,
	executorClient executor.Client,
) Processor {
	return &taskProcessor{
		logger: logger,

		cellID:         cellID,
		bbs:            bbs,
		executorClient: executorClient,
	}
}

func (p *taskProcessor) Process(container executor.Container) {
	if container.State != executor.StateCompleted {
		return
	}

	pLog := p.logger.Session("process-task", lager.Data{
		"container": container.Guid,
	})

	taskGuid := container.Guid
	taskFailed := container.RunResult.Failed
	failureReason := container.RunResult.FailureReason

	var taskResult string
	var err error

	if !taskFailed {
		resultFile := container.Tags[rep.ResultFileTag]
		if resultFile != "" {
			taskResult, err = p.getResultFile(taskGuid, resultFile)
			if err != nil {
				pLog.Error("failed-to-fetch-result", err)

				taskFailed = true
				failureReason = "failed to fetch result"
			}
		}
	}

	err = p.bbs.CompleteTask(taskGuid, p.cellID, taskFailed, failureReason, taskResult)
	if err != nil {
		pLog.Error("failed-to-mark-complete", err)
	}

	err = p.executorClient.DeleteContainer(container.Guid)
	if err != nil {
		pLog.Error("failed-to-delete-container", err)
	} else {
		pLog.Info("completed")
	}
}

func (p *taskProcessor) getResultFile(guid, resultFile string) (string, error) {
	stream, err := p.executorClient.GetFiles(guid, resultFile)
	if err != nil {
		return "", err
	}
	defer stream.Close()

	tarReader := tar.NewReader(stream)

	_, err = tarReader.Next()
	if err != nil {
		return "", err
	}

	buf := make([]byte, MAX_RESULT_SIZE+1)
	n, err := tarReader.Read(buf)
	if n > MAX_RESULT_SIZE {
		return "", err
	}

	return string(buf[:n]), nil
}
