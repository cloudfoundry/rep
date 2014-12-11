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

func ActualLRPFromContainer(container executor.Container, cellId, executorHost string) (*models.ActualLRP, error) {
	if container.Tags == nil {
		return nil, ErrContainerMissingTags
	}

	processIndex, err := strconv.Atoi(container.Tags[ProcessIndexTag])
	if err != nil {
		return nil, ErrInvalidProcessIndex
	}

	actualLrp := models.NewActualLRP(
		container.Tags[ProcessGuidTag],
		container.Guid,
		cellId,
		container.Tags[DomainTag],
		processIndex,
		"",
	)

	err = actualLrp.Validate()
	if err != nil {
		return nil, err
	}

	ports := []models.PortMapping{}
	for _, portMapping := range container.Ports {
		ports = append(ports, models.PortMapping{
			ContainerPort: portMapping.ContainerPort,
			HostPort:      portMapping.HostPort,
		})
	}

	actualLrp.Ports = ports
	actualLrp.Host = executorHost

	return &actualLrp, nil
}
