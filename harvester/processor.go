package harvester

import (
	"archive/tar"
	"fmt"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/pivotal-golang/lager"
)

type Processor interface {
	Process(executor.Container)
}

type processor struct {
	logger lager.Logger

	bbs            bbs.RepBBS
	executorClient executor.Client
}

const MAX_RESULT_SIZE = 1024 * 10

func NewProcessor(
	logger lager.Logger,
	bbs bbs.RepBBS,
	executorClient executor.Client,
) Processor {
	return &processor{
		logger: logger,

		bbs:            bbs,
		executorClient: executorClient,
	}
}

func (p *processor) Process(container executor.Container) {
	pLog := p.logger.Session("process", lager.Data{
		"container": container.Guid,
	})

	taskGuid := container.Guid
	taskFailed := container.RunResult.Failed
	failureReason := container.RunResult.FailureReason

	var taskResult string
	var err error

	if !taskFailed {
		resultFile := container.Tags[ResultFileTag]
		if resultFile != "" {
			taskResult, err = p.getResultFile(taskGuid, resultFile)
			if err != nil {
				taskFailed = true
				failureReason = fmt.Sprintf("failed to fetch result: %s", err.Error())
			}
		}
	}

	err = p.bbs.CompleteTask(taskGuid, taskFailed, failureReason, taskResult)
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

func (p *processor) getResultFile(guid, resultFile string) (string, error) {
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
