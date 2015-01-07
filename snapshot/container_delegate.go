package snapshot

import (
	"archive/tar"
	"errors"
	"fmt"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/pivotal-golang/lager"
)

const MAX_RESULT_SIZE = 1024 * 10

var ErrResultFileTooLarge = errors.New(
	fmt.Sprintf("result file is too large (over %d bytes)", MAX_RESULT_SIZE),
)

//go:generate counterfeiter -o fake_snapshot/fake_container_delegate.go container_delegate.go ContainerDelegate

type ContainerDelegate interface {
	RunContainer(logger lager.Logger, guid string) bool
	StopContainer(logger lager.Logger, guid string) bool
	DeleteContainer(logger lager.Logger, guid string) bool
	FetchContainerResult(logger lager.Logger, guid string, filename string) (string, error)
}

type containerDelegate struct {
	client executor.Client
}

func NewContainerDelegate(client executor.Client) ContainerDelegate {
	return &containerDelegate{
		client: client,
	}
}

func (d *containerDelegate) RunContainer(logger lager.Logger, guid string) bool {
	logger.Info("running-container")
	err := d.client.RunContainer(guid)
	if err != nil {
		logger.Error("failed-running-container", err)
		d.DeleteContainer(logger, guid)
		return false
	}
	logger.Info("succeeded-running-container")
	return true
}

func (d *containerDelegate) StopContainer(logger lager.Logger, guid string) bool {
	logger.Info("stopping-container")
	err := d.client.StopContainer(guid)
	if err != nil {
		logger.Error("failed-stopping-container", err)
		return false
	}
	logger.Info("succeeded-stopping-container")
	return true
}

func (d *containerDelegate) DeleteContainer(logger lager.Logger, guid string) bool {
	logger.Info("deleting-container")
	err := d.client.DeleteContainer(guid)
	if err != nil {
		logger.Error("failed-deleting-container", err)
		return false
	}
	logger.Info("succeeded-deleting-container")
	return true
}

func (d *containerDelegate) FetchContainerResult(logger lager.Logger, guid string, filename string) (string, error) {
	stream, err := d.client.GetFiles(guid, filename)
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
		return "", ErrResultFileTooLarge
	}

	return string(buf[:n]), nil
}
