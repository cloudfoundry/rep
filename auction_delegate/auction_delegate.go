package auction_delegate

import (
	"strconv"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/executor/client"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	steno "github.com/cloudfoundry/gosteno"
)

const ProcessGuidMetadataKey = "process-guid"
const IndexMetadataKey = "index"

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
		if container.Metadata[ProcessGuidMetadataKey] == processGuid {
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

	indexAsString := strconv.Itoa(index)
	instanceGuids := []string{}
	for _, container := range containers {
		if container.Metadata[ProcessGuidMetadataKey] == processGuid && container.Metadata[IndexMetadataKey] == indexAsString {
			instanceGuids = append(instanceGuids, container.Guid)
		}
	}
	return instanceGuids, nil
}

func (a *AuctionDelegate) Reserve(auctionInfo auctiontypes.StartAuctionInfo) error {
	a.logger.Debugd(map[string]interface{}{
		"auction-info": auctionInfo,
	}, "auction-delegate.reserve")

	_, err := a.client.AllocateContainer(auctionInfo.InstanceGuid, api.ContainerAllocationRequest{
		MemoryMB: auctionInfo.MemoryMB,
		DiskMB:   auctionInfo.DiskMB,
		Metadata: map[string]string{
			ProcessGuidMetadataKey: auctionInfo.ProcessGuid,
			IndexMetadataKey:       strconv.Itoa(auctionInfo.Index),
		},
	})
	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.reserve.failed")
	}
	return err
}

func (a *AuctionDelegate) ReleaseReservation(auctionInfo auctiontypes.StartAuctionInfo) error {
	err := a.client.DeleteContainer(auctionInfo.InstanceGuid)
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

	container, err := a.client.InitializeContainer(startAuction.InstanceGuid, api.ContainerInitializationRequest{
		Ports: a.convertPortMappings(startAuction.Ports),
		Log:   startAuction.Log,
	})
	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.initialize-container-request.failed")
		a.client.DeleteContainer(startAuction.InstanceGuid)
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
		a.client.DeleteContainer(startAuction.InstanceGuid)
		return err
	}

	err = a.client.Run(startAuction.InstanceGuid, api.ContainerRunRequest{
		Actions: startAuction.Actions,
	})
	if err != nil {
		a.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "auction-delegate.run-actions.failed")
		a.client.DeleteContainer(startAuction.InstanceGuid)
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
