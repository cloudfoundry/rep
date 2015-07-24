package rep

import (
	"encoding/json"
	"errors"
	"strconv"

	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/executor"
	oldmodels "github.com/cloudfoundry-incubator/runtime-schema/models"
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

func ActualLRPKeyFromContainer(container executor.Container) (oldmodels.ActualLRPKey, error) {
	if container.Tags == nil {
		return oldmodels.ActualLRPKey{}, ErrContainerMissingTags
	}

	processIndex, err := strconv.Atoi(container.Tags[ProcessIndexTag])
	if err != nil {
		return oldmodels.ActualLRPKey{}, ErrInvalidProcessIndex
	}

	actualLRPKey := oldmodels.NewActualLRPKey(
		container.Tags[ProcessGuidTag],
		processIndex,
		container.Tags[DomainTag],
	)

	err = actualLRPKey.Validate()
	if err != nil {
		return oldmodels.ActualLRPKey{}, err
	}

	return actualLRPKey, nil
}

func ActualLRPInstanceKeyFromContainer(container executor.Container, cellID string) (oldmodels.ActualLRPInstanceKey, error) {
	if container.Tags == nil {
		return oldmodels.ActualLRPInstanceKey{}, ErrContainerMissingTags
	}

	actualLRPInstanceKey := oldmodels.NewActualLRPInstanceKey(
		container.Tags[InstanceGuidTag],
		cellID,
	)

	err := actualLRPInstanceKey.Validate()
	if err != nil {
		return oldmodels.ActualLRPInstanceKey{}, err
	}

	return actualLRPInstanceKey, nil
}

func ActualLRPNetInfoFromContainer(container executor.Container) (*models.ActualLRPNetInfo, error) {
	ports := []*models.PortMapping{}
	for _, portMapping := range container.Ports {
		ports = append(ports, models.NewPortMapping(uint32(portMapping.HostPort), uint32(portMapping.ContainerPort)))
	}

	actualLRPNetInfo := models.NewActualLRPNetInfo(container.ExternalIP, ports...)

	err := actualLRPNetInfo.Validate()
	if err != nil {
		return nil, err
	}

	return &actualLRPNetInfo, nil
}

func LegacyActualLRPNetInfoFromContainer(container executor.Container) (oldmodels.ActualLRPNetInfo, error) {
	ports := []oldmodels.PortMapping{}
	for _, portMapping := range container.Ports {
		ports = append(ports, oldmodels.PortMapping{
			ContainerPort: portMapping.ContainerPort,
			HostPort:      portMapping.HostPort,
		})
	}

	actualLRPNetInfo := oldmodels.NewActualLRPNetInfo(container.ExternalIP, ports)

	err := actualLRPNetInfo.Validate()
	if err != nil {
		return oldmodels.ActualLRPNetInfo{}, err
	}

	return actualLRPNetInfo, nil
}

func LRPContainerGuid(processGuid, instanceGuid string) string {
	return processGuid + "-" + instanceGuid
}

type StackPathMap map[string]string

func UnmarshalStackPathMap(payload []byte) (StackPathMap, error) {
	stackPathMap := StackPathMap{}
	err := json.Unmarshal(payload, &stackPathMap)
	return stackPathMap, err
}
