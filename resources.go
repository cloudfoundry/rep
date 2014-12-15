package rep

import (
	"errors"
	"strconv"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

const (
	LifecycleTag  = "lifecycle"
	ResultFileTag = "result-file"
	DomainTag     = "domain"

	TaskLifecycle = "task"
	LRPLifecycle  = "lrp"

	ProcessGuidTag  = "process-guid"
	ProcessIndexTag = "process-index"
)

var (
	ErrContainerMissingTags = errors.New("container is missing tags")
	ErrInvalidProcessIndex  = errors.New("container does not have a valid process index")
)

func ActualLRPFromContainer(
	container executor.Container,
	cellID string,
	executorHost string,
) (models.ActualLRPKey, models.ActualLRPContainerKey, models.ActualLRPNetInfo, error) {
	if container.Tags == nil {
		return models.ActualLRPKey{}, models.ActualLRPContainerKey{}, models.ActualLRPNetInfo{}, ErrContainerMissingTags
	}

	processIndex, err := strconv.Atoi(container.Tags[ProcessIndexTag])
	if err != nil {
		return models.ActualLRPKey{}, models.ActualLRPContainerKey{}, models.ActualLRPNetInfo{}, ErrInvalidProcessIndex
	}

	actualLRPKey := models.NewActualLRPKey(
		container.Tags[ProcessGuidTag],
		processIndex,
		container.Tags[DomainTag],
	)

	err = actualLRPKey.Validate()
	if err != nil {
		return models.ActualLRPKey{}, models.ActualLRPContainerKey{}, models.ActualLRPNetInfo{}, err
	}

	actualLRPContainerKey := models.NewActualLRPContainerKey(
		container.Guid,
		cellID,
	)

	err = actualLRPContainerKey.Validate()
	if err != nil {
		return models.ActualLRPKey{}, models.ActualLRPContainerKey{}, models.ActualLRPNetInfo{}, err
	}

	ports := []models.PortMapping{}
	for _, portMapping := range container.Ports {
		ports = append(ports, models.PortMapping{
			ContainerPort: portMapping.ContainerPort,
			HostPort:      portMapping.HostPort,
		})
	}

	actualLRPNetInfo := models.NewActualLRPNetInfo(executorHost, ports)

	return actualLRPKey, actualLRPContainerKey, actualLRPNetInfo, nil
}
