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
	InstanceGuidTag = "instance-guid"
	ProcessIndexTag = "process-index"
)

var (
	ErrContainerMissingTags = errors.New("container is missing tags")
	ErrInvalidProcessIndex  = errors.New("container does not have a valid process index")
)

func ActualLRPKeyFromContainer(container executor.Container) (models.ActualLRPKey, error) {
	if container.Tags == nil {
		return models.ActualLRPKey{}, ErrContainerMissingTags
	}

	processIndex, err := strconv.Atoi(container.Tags[ProcessIndexTag])
	if err != nil {
		return models.ActualLRPKey{}, ErrInvalidProcessIndex
	}

	actualLRPKey := models.NewActualLRPKey(
		container.Tags[ProcessGuidTag],
		processIndex,
		container.Tags[DomainTag],
	)

	err = actualLRPKey.Validate()
	if err != nil {
		return models.ActualLRPKey{}, err
	}

	return actualLRPKey, nil
}

func ActualLRPInstanceKeyFromContainer(container executor.Container, cellID string) (models.ActualLRPInstanceKey, error) {
	if container.Tags == nil {
		return models.ActualLRPInstanceKey{}, ErrContainerMissingTags
	}

	actualLRPInstanceKey := models.NewActualLRPInstanceKey(
		container.Tags[InstanceGuidTag],
		cellID,
	)

	err := actualLRPInstanceKey.Validate()
	if err != nil {
		return models.ActualLRPInstanceKey{}, err
	}

	return actualLRPInstanceKey, nil
}

func ActualLRPNetInfoFromContainer(container executor.Container) (models.ActualLRPNetInfo, error) {
	ports := []models.PortMapping{}
	for _, portMapping := range container.Ports {
		ports = append(ports, models.PortMapping{
			ContainerPort: portMapping.ContainerPort,
			HostPort:      portMapping.HostPort,
		})
	}

	actualLRPNetInfo := models.NewActualLRPNetInfo(container.ExternalIP, ports)

	err := actualLRPNetInfo.Validate()
	if err != nil {
		return models.ActualLRPNetInfo{}, err
	}

	return actualLRPNetInfo, nil
}

func LRPContainerGuid(processGuid, instanceGuid string) string {
	return processGuid + "-" + instanceGuid
}
