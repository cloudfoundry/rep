package auction_delegate

import (
	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	executorapi "github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

type AuctionDelegate struct {
	executorID string
	lrpStopper lrp_stopper.LRPStopper
	bbs        Bbs.RepBBS
	client     executorapi.Client
	logger     lager.Logger
}

func New(executorID string, lrpStopper lrp_stopper.LRPStopper, bbs Bbs.RepBBS, client executorapi.Client, logger lager.Logger) *AuctionDelegate {
	return &AuctionDelegate{
		executorID: executorID,
		lrpStopper: lrpStopper,
		bbs:        bbs,
		client:     client,
		logger:     logger.Session("auction-delegate"),
	}
}

func (a *AuctionDelegate) RemainingResources() (auctiontypes.Resources, error) {
	resources, err := a.fetchResourcesVia(a.client.RemainingResources)
	if err != nil {
		a.logger.Error("failed-to-get-remaining-resource", err)
	}
	return resources, err
}

func (a *AuctionDelegate) TotalResources() (auctiontypes.Resources, error) {
	resources, err := a.fetchResourcesVia(a.client.TotalResources)
	if err != nil {
		a.logger.Error("failed-to-get-total-resources", err)
	}
	return resources, err
}

func (a *AuctionDelegate) NumInstancesForProcessGuid(processGuid string) (int, error) {
	containers, err := a.client.ListContainers()
	if err != nil {
		a.logger.Error("failed-to-list-containers", err)
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
		a.logger.Error("failed-to-list-containers", err)
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
	reserveLog := a.logger.Session("reservation")

	reserveLog.Info("reserve", lager.Data{
		"auction-info": auctionInfo,
	})

	_, err := a.client.AllocateContainer(auctionInfo.LRPIdentifier().OpaqueID(), executorapi.ContainerAllocationRequest{
		MemoryMB: auctionInfo.MemoryMB,
		DiskMB:   auctionInfo.DiskMB,
	})
	if err != nil {
		reserveLog.Error("failed-to-reserve", err)
	}

	return err
}

func (a *AuctionDelegate) ReleaseReservation(auctionInfo auctiontypes.StartAuctionInfo) error {
	err := a.client.DeleteContainer(auctionInfo.LRPIdentifier().OpaqueID())
	if err != nil {
		a.logger.Error("failed-to-release-reservation", err)
	}

	return err
}

func (a *AuctionDelegate) Run(startAuction models.LRPStartAuction) error {
	auctionLog := a.logger.Session("run")

	auctionLog.Info("start", lager.Data{
		"start-auction": startAuction,
	})

	containerGuid := startAuction.LRPIdentifier().OpaqueID()

	_, err := a.client.InitializeContainer(containerGuid, executorapi.ContainerInitializationRequest{
		Ports: a.convertPortMappings(startAuction.Ports),
		Log:   startAuction.Log,
	})
	if err != nil {
		auctionLog.Error("failed-to-initialize-container", err)
		a.client.DeleteContainer(containerGuid)
		return err
	}

	lrp := models.ActualLRP{
		ProcessGuid:  startAuction.ProcessGuid,
		InstanceGuid: startAuction.InstanceGuid,
		Index:        startAuction.Index,
	}
	err = a.bbs.ReportActualLRPAsStarting(lrp, a.executorID)

	if err != nil {
		auctionLog.Error("failed-to-mark-starting", err)
		a.client.DeleteContainer(containerGuid)
		return err
	}

	err = a.client.Run(containerGuid, executorapi.ContainerRunRequest{
		Actions: startAuction.Actions,
	})
	if err != nil {
		auctionLog.Error("failed-to-run-actions", err)
		a.client.DeleteContainer(containerGuid)
		a.bbs.RemoveActualLRP(lrp)
		return err
	}

	return nil
}

func (a *AuctionDelegate) Stop(stopInstance models.StopLRPInstance) error {
	a.logger.Info("stop-instance", lager.Data{
		"stop-instance": stopInstance,
	})

	return a.lrpStopper.StopInstance(stopInstance)
}

func (a *AuctionDelegate) convertPortMappings(portMappings []models.PortMapping) []executorapi.PortMapping {
	out := []executorapi.PortMapping{}
	for _, portMapping := range portMappings {
		out = append(out, executorapi.PortMapping{
			ContainerPort: portMapping.ContainerPort,
			HostPort:      portMapping.HostPort,
		})
	}

	return out
}

func (a *AuctionDelegate) fetchResourcesVia(fetcher func() (executorapi.ExecutorResources, error)) (auctiontypes.Resources, error) {
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
