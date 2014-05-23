package auction_delegate

import (
	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/executor/client"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	steno "github.com/cloudfoundry/gosteno"
)

const ProcessGuidMetadataKey = "process-guid"

type AuctionDelegate struct {
	bbs    Bbs.RepBBS
	client client.Client
	logger *steno.Logger
}

func New(bbs Bbs.RepBBS, client client.Client, logger *steno.Logger) *AuctionDelegate {
	return &AuctionDelegate{
		bbs:    bbs,
		client: client,
		logger: logger,
	}
}

func (a *AuctionDelegate) RemainingResources() (auctiontypes.Resources, error) {
	resources, err := a.fetchResourcesVia(a.client.RemainingResources)
	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.get-remaining-resources.failed")
	}
	return resources, err
}

func (a *AuctionDelegate) TotalResources() (auctiontypes.Resources, error) {
	resources, err := a.fetchResourcesVia(a.client.TotalResources)
	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.get-remaining-resources.failed")
	}
	return resources, err
}

func (a *AuctionDelegate) NumInstancesForAppGuid(guid string) (int, error) {
	containers, err := a.client.ListContainers()
	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.list-containers.failed")
		return 0, err
	}

	count := 0
	for _, container := range containers {
		if container.Metadata[ProcessGuidMetadataKey] == guid {
			count++
		}
	}
	return count, nil
}

func (a *AuctionDelegate) Reserve(instance auctiontypes.LRPAuctionInfo) error {
	a.logger.Debugd(map[string]interface{}{
		"instance": instance,
	}, "auction-delegate.reserve")

	_, err := a.client.AllocateContainer(instance.InstanceGuid, api.ContainerAllocationRequest{
		MemoryMB: instance.MemoryMB,
		DiskMB:   instance.DiskMB,
		Metadata: map[string]string{
			ProcessGuidMetadataKey: instance.AppGuid,
		},
	})
	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.reserve.failed")
	}
	return err
}

func (a *AuctionDelegate) ReleaseReservation(instance auctiontypes.LRPAuctionInfo) error {
	err := a.client.DeleteContainer(instance.InstanceGuid)
	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.release-reserve.failed")
	}
	return err
}

func (a *AuctionDelegate) Run(instance models.LRPStartAuction) error {
	a.logger.Infod(map[string]interface{}{
		"instance": instance,
	}, "auction-delegate.run")

	err := a.client.InitializeContainer(instance.InstanceGuid, api.ContainerInitializationRequest{
		Ports: a.convertPortMappings(instance.Ports),
		Log:   instance.Log,
	})
	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.initialize-container-request.failed")
		a.client.DeleteContainer(instance.InstanceGuid)
		return err
	}

	lrp := models.LRP{
		ProcessGuid:  instance.ProcessGuid,
		InstanceGuid: instance.InstanceGuid,
		Index:        instance.Index,
	}
	err = a.bbs.ReportActualLongRunningProcessAsStarting(lrp)

	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.mark-starting.failed")
		a.client.DeleteContainer(instance.InstanceGuid)
		return err
	}

	err = a.client.Run(instance.InstanceGuid, api.ContainerRunRequest{
		Actions: instance.Actions,
	})
	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.run-actions.failed")
		a.client.DeleteContainer(instance.InstanceGuid)
		a.bbs.RemoveActualLongRunningProcess(lrp)
		return err
	}

	return nil
}

func (a *AuctionDelegate) convertPortMappings(portMappings []models.PortMapping) []api.PortMapping {
	out := []api.PortMapping{}
	for _, portMapping := range portMappings {
		out = append(out, api.PortMapping{
			ContainerPort: portMapping.ContainerPort,
			HostPort:      portMapping.HostPort,
		})
	}

	return out
}

func (a *AuctionDelegate) fetchResourcesVia(fetcher func() (api.ExecutorResources, error)) (auctiontypes.Resources, error) {
	resources, err := fetcher()
	if err != nil {
		return auctiontypes.Resources{}, err
	}
	return auctiontypes.Resources{
		MemoryMB:   resources.MemoryMB,
		DiskMB:     resources.DiskMB,
		Containers: resources.Containers,
	}, nil
}
