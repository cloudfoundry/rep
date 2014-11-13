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
)

const (
	ProcessGuidTag  = "process-guid"
	ProcessIndexTag = "process-index"
)

func ActualLRPFromContainer(container executor.Container, cellId, executorHost string) (models.ActualLRP, error) {
	if container.Tags == nil {
		return models.ActualLRP{}, errors.New("container is missing tags")
	}

	processIndex, err := strconv.Atoi(container.Tags[ProcessIndexTag])
	if err != nil {
		return models.ActualLRP{}, errors.New("container does not have a valid process index")
	}

	actualLrp, err := models.NewActualLRP(
		container.Tags[ProcessGuidTag],
		container.Guid,
		cellId,
		container.Tags[DomainTag],
		processIndex,
	)
	if err != nil {
		return models.ActualLRP{}, err
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

	return actualLrp, nil
}
