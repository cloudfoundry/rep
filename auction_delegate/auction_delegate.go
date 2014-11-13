package auction_delegate

import (
	"strconv"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

type AuctionDelegate struct {
	cellID     string
	lrpStopper lrp_stopper.LRPStopper
	bbs        Bbs.RepBBS
	client     executor.Client
	logger     lager.Logger
}

func New(cellID string, lrpStopper lrp_stopper.LRPStopper, bbs Bbs.RepBBS, client executor.Client, logger lager.Logger) *AuctionDelegate {
	return &AuctionDelegate{
		cellID:     cellID,
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
	containers, err := a.client.ListContainers(executor.Tags{
		rep.ProcessGuidTag: processGuid,
	})
	if err != nil {
		a.logger.Error("failed-to-list-containers", err)
		return 0, err
	}

	return len(containers), nil
}

func (a *AuctionDelegate) InstanceGuidsForProcessGuidAndIndex(processGuid string, index int) ([]string, error) {
	containers, err := a.client.ListContainers(executor.Tags{
		rep.ProcessGuidTag:  processGuid,
		rep.ProcessIndexTag: strconv.Itoa(index),
	})
	if err != nil {
		a.logger.Error("failed-to-list-containers", err)
		return []string{}, err
	}

	instanceGuids := []string{}
	for _, container := range containers {
		instanceGuids = append(instanceGuids, container.Guid)
	}

	return instanceGuids, nil
}

func (a *AuctionDelegate) Reserve(startAuction models.LRPStartAuction) error {
	reserveLog := a.logger.Session("reservation")

	reserveLog.Info("reserve", lager.Data{
		"start-auction": startAuction,
	})

	_, err := a.client.AllocateContainer(executor.Container{
		Guid: startAuction.InstanceGuid,

		Tags: executor.Tags{
			rep.LifecycleTag:    rep.LRPLifecycle,
			rep.DomainTag:       startAuction.DesiredLRP.Domain,
			rep.ProcessGuidTag:  startAuction.DesiredLRP.ProcessGuid,
			rep.ProcessIndexTag: strconv.Itoa(startAuction.Index),
		},

		MemoryMB:   startAuction.DesiredLRP.MemoryMB,
		DiskMB:     startAuction.DesiredLRP.DiskMB,
		CPUWeight:  startAuction.DesiredLRP.CPUWeight,
		RootFSPath: startAuction.DesiredLRP.RootFSPath,
		Ports:      a.convertPortMappings(startAuction.DesiredLRP.Ports),

		Log: executor.LogConfig{
			Guid:       startAuction.DesiredLRP.LogGuid,
			SourceName: startAuction.DesiredLRP.LogSource,
			Index:      &startAuction.Index,
		},

		Setup:   startAuction.DesiredLRP.Setup,
		Action:  startAuction.DesiredLRP.Action,
		Monitor: startAuction.DesiredLRP.Monitor,

		Env: append([]executor.EnvironmentVariable{
			{Name: "CF_INSTANCE_GUID", Value: startAuction.InstanceGuid},
			{Name: "CF_INSTANCE_INDEX", Value: strconv.Itoa(startAuction.Index)},
		}, executor.EnvironmentVariablesFromModel(startAuction.DesiredLRP.EnvironmentVariables)...),
	})
	if err != nil {
		reserveLog.Error("failed-to-reserve", err)
	}

	return err
}

func (a *AuctionDelegate) ReleaseReservation(startAuction models.LRPStartAuction) error {
	err := a.client.DeleteContainer(startAuction.InstanceGuid)
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

	containerGuid := startAuction.InstanceGuid

	lrp, err := a.bbs.ReportActualLRPAsStarting(startAuction.DesiredLRP.ProcessGuid, startAuction.InstanceGuid, a.cellID, startAuction.DesiredLRP.Domain, startAuction.Index)
	if err != nil {
		auctionLog.Error("failed-to-mark-starting", err)
		a.client.DeleteContainer(containerGuid)
		return err
	}

	err = a.client.RunContainer(containerGuid)
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

func (a *AuctionDelegate) convertPortMappings(portMappings []models.PortMapping) []executor.PortMapping {
	out := []executor.PortMapping{}
	for _, portMapping := range portMappings {
		out = append(out, executor.PortMapping{
			ContainerPort: portMapping.ContainerPort,
			HostPort:      portMapping.HostPort,
		})
	}

	return out
}

func (a *AuctionDelegate) fetchResourcesVia(fetcher func() (executor.ExecutorResources, error)) (auctiontypes.Resources, error) {
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
