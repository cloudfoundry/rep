package auction_delegate

import (
	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/executor/client"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	steno "github.com/cloudfoundry/gosteno"
)

type AuctionDelegate struct {
	lrpStopper lrp_stopper.LRPStopper
	bbs        Bbs.RepBBS
	client     client.Client
	logger     *steno.Logger
}

func New(lrpStopper lrp_stopper.LRPStopper, bbs Bbs.RepBBS, client client.Client, logger *steno.Logger) *AuctionDelegate {
	return &AuctionDelegate{
		lrpStopper: lrpStopper,
		bbs:        bbs,
		client:     client,
		logger:     logger,
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

func (a *AuctionDelegate) NumInstancesForProcessGuid(processGuid string) (int, error) {
	containers, err := a.client.ListContainers()
	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.list-containers.failed")
		return 0, err
	}

	count := 0
	for _, container := range containers {
		identifier, err := models.LRPIdentifierFromOpaqueID(container.Guid)
		if err != nil {
			continue
		}

		if identifier.ProcessGuid == processGuid {
			count++
		}
	}
	return count, nil
}

func (a *AuctionDelegate) InstanceGuidsForProcessGuidAndIndex(processGuid string, index int) ([]string, error) {
	containers, err := a.client.ListContainers()
	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.list-containers.failed")
		return []string{}, err
	}

	instanceGuids := []string{}
	for _, container := range containers {
		identifier, err := models.LRPIdentifierFromOpaqueID(container.Guid)
		if err != nil {
			continue
		}

		if identifier.ProcessGuid == processGuid && identifier.Index == index {
			instanceGuids = append(instanceGuids, identifier.InstanceGuid)
		}
	}
	return instanceGuids, nil
}

func (a *AuctionDelegate) Reserve(auctionInfo auctiontypes.StartAuctionInfo) error {
	a.logger.Debugd(map[string]interface{}{
		"auction-info": auctionInfo,
	}, "auction-delegate.reserve")

	_, err := a.client.AllocateContainer(auctionInfo.LRPIdentifier().OpaqueID(), api.ContainerAllocationRequest{
		MemoryMB: auctionInfo.MemoryMB,
		DiskMB:   auctionInfo.DiskMB,
	})
	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.reserve.failed")
	}
	return err
}

func (a *AuctionDelegate) ReleaseReservation(auctionInfo auctiontypes.StartAuctionInfo) error {
	err := a.client.DeleteContainer(auctionInfo.LRPIdentifier().OpaqueID())
	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.release-reserve.failed")
	}
	return err
}

func (a *AuctionDelegate) Run(startAuction models.LRPStartAuction) error {
	a.logger.Infod(map[string]interface{}{
		"start-auction": startAuction,
	}, "auction-delegate.run")

	containerGuid := startAuction.LRPIdentifier().OpaqueID()

	container, err := a.client.InitializeContainer(containerGuid, api.ContainerInitializationRequest{
		Ports: a.convertPortMappings(startAuction.Ports),
		Log:   startAuction.Log,
	})
	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.initialize-container-request.failed")
		a.client.DeleteContainer(containerGuid)
		return err
	}

	lrp := models.ActualLRP{
		ProcessGuid:  startAuction.ProcessGuid,
		InstanceGuid: startAuction.InstanceGuid,
		Index:        startAuction.Index,
	}
	err = a.bbs.ReportActualLRPAsStarting(lrp, container.ExecutorGuid)

	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.mark-starting.failed")
		a.client.DeleteContainer(containerGuid)
		return err
	}

	err = a.client.Run(containerGuid, api.ContainerRunRequest{
		Actions: startAuction.Actions,
	})
	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.run-actions.failed")
		a.client.DeleteContainer(containerGuid)
		a.bbs.RemoveActualLRP(lrp)
		return err
	}

	return nil
}

func (a *AuctionDelegate) Stop(stopInstance models.StopLRPInstance) error {
	a.logger.Infod(map[string]interface{}{
		"stop-instance": stopInstance,
	}, "auction-delegate.stop")

	return a.lrpStopper.StopInstance(stopInstance)
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
