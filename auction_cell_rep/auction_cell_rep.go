package auction_cell_rep

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

type AuctionCellRep struct {
	cellID     string
	stack      string
	lrpStopper lrp_stopper.LRPStopper
	bbs        Bbs.RepBBS
	client     executor.Client
	logger     lager.Logger
}

func New(cellID string, stack string, lrpStopper lrp_stopper.LRPStopper, bbs Bbs.RepBBS, client executor.Client, logger lager.Logger) *AuctionCellRep {
	return &AuctionCellRep{
		cellID:     cellID,
		stack:      stack,
		lrpStopper: lrpStopper,
		bbs:        bbs,
		client:     client,
		logger:     logger.Session("auction-delegate"),
	}
}

func (a *AuctionCellRep) State() (auctiontypes.CellState, error) {
	logger := a.logger.Session("auction-state")
	logger.Info("providing")

	totalResources, err := a.fetchResourcesVia(a.client.TotalResources)
	if err != nil {
		logger.Error("failed-to-get-total-resources", err)
		return auctiontypes.CellState{}, err
	}

	availableResources, err := a.fetchResourcesVia(a.client.RemainingResources)
	if err != nil {
		logger.Error("failed-to-get-remaining-resource", err)
		return auctiontypes.CellState{}, err
	}

	lrpContainers, err := a.client.ListContainers(executor.Tags{
		rep.LifecycleTag: rep.LRPLifecycle,
	})

	if err != nil {
		logger.Error("failed-to-fetch-containers", err)
		return auctiontypes.CellState{}, err
	}

	lrps := []auctiontypes.LRP{}

	for _, container := range lrpContainers {
		index, _ := strconv.Atoi(container.Tags[rep.ProcessIndexTag])
		lrp := auctiontypes.LRP{
			ProcessGuid:  container.Tags[rep.ProcessGuidTag],
			InstanceGuid: container.Guid,
			Index:        index,
			MemoryMB:     container.MemoryMB,
			DiskMB:       container.DiskMB,
		}
		lrps = append(lrps, lrp)
	}

	state := auctiontypes.CellState{
		Stack:              a.stack,
		AvailableResources: availableResources,
		TotalResources:     totalResources,
		LRPs:               lrps,
	}

	a.logger.Session("provided", lager.Data{"state": state})

	return state, nil
}

func (a *AuctionCellRep) Perform(work auctiontypes.Work) (auctiontypes.Work, error) {
	var failedWork = auctiontypes.Work{}

	logger := a.logger.Session("auction-work", lager.Data{
		"starts": len(work.Starts),
		"stops":  len(work.Stops),
	})

	for _, stop := range work.Stops {
		stopLogger := logger.Session("stop-instance", lager.Data{"process-guid": stop.ProcessGuid, "instance-guid": stop.InstanceGuid, "index": stop.Index})
		stopLogger.Info("stopping")
		err := a.stop(stop)
		if err != nil {
			stopLogger.Error("failed-to-stop", err)
			failedWork.Stops = append(failedWork.Stops, stop)
		} else {
			stopLogger.Info("stopped")
		}
	}

	for _, start := range work.Starts {
		startLogger := logger.Session("start-instance", lager.Data{
			"process-guid":  start.DesiredLRP.ProcessGuid,
			"instance-guid": start.InstanceGuid,
			"index":         start.Index,
			"memory-mb":     start.DesiredLRP.MemoryMB,
			"disk-mb":       start.DesiredLRP.DiskMB,
		})
		startLogger.Info("starting")
		err := a.start(start, startLogger)
		if err != nil {
			startLogger.Error("failed-to-start", err)
			failedWork.Starts = append(failedWork.Starts, start)
		} else {
			startLogger.Info("started")
		}
	}

	return failedWork, nil
}

func (a *AuctionCellRep) start(startAuction models.LRPStartAuction, logger lager.Logger) error {
	logger.Info("reserving")

	containerGuid := startAuction.InstanceGuid

	_, err := a.client.AllocateContainer(executor.Container{
		Guid: containerGuid,

		Tags: executor.Tags{
			rep.LifecycleTag:    rep.LRPLifecycle,
			rep.DomainTag:       startAuction.DesiredLRP.Domain,
			rep.ProcessGuidTag:  startAuction.DesiredLRP.ProcessGuid,
			rep.ProcessIndexTag: strconv.Itoa(startAuction.Index),
		},

		MemoryMB:     startAuction.DesiredLRP.MemoryMB,
		DiskMB:       startAuction.DesiredLRP.DiskMB,
		CPUWeight:    startAuction.DesiredLRP.CPUWeight,
		RootFSPath:   startAuction.DesiredLRP.RootFSPath,
		Ports:        a.convertPortMappings(startAuction.DesiredLRP.Ports),
		StartTimeout: startAuction.DesiredLRP.StartTimeout,

		Log: executor.LogConfig{
			Guid:       startAuction.DesiredLRP.LogGuid,
			SourceName: startAuction.DesiredLRP.LogSource,
			Index:      &startAuction.Index,
		},

		Setup:   startAuction.DesiredLRP.Setup,
		Action:  startAuction.DesiredLRP.Action,
		Monitor: startAuction.DesiredLRP.Monitor,

		Env: append([]executor.EnvironmentVariable{
			{Name: "INSTANCE_GUID", Value: startAuction.InstanceGuid},
			{Name: "INSTANCE_INDEX", Value: strconv.Itoa(startAuction.Index)},
		}, executor.EnvironmentVariablesFromModel(startAuction.DesiredLRP.EnvironmentVariables)...),
	})

	if err != nil {
		return err
	}

	logger.Info("announcing-to-bbs")
	lrp, err := a.bbs.ReportActualLRPAsStarting(startAuction.DesiredLRP.ProcessGuid, startAuction.InstanceGuid, a.cellID, startAuction.DesiredLRP.Domain, startAuction.Index)
	if err != nil {
		a.client.DeleteContainer(containerGuid)
		return err
	}

	logger.Info("running")
	err = a.client.RunContainer(containerGuid)
	if err != nil {
		a.client.DeleteContainer(containerGuid)
		a.bbs.RemoveActualLRP(lrp)
		return err
	}

	return nil
}

func (a *AuctionCellRep) stop(lrp models.ActualLRP) error {
	return a.lrpStopper.StopInstance(lrp)
}

func (a *AuctionCellRep) convertPortMappings(containerPorts []uint32) []executor.PortMapping {
	out := []executor.PortMapping{}
	for _, port := range containerPorts {
		out = append(out, executor.PortMapping{
			ContainerPort: port,
		})
	}

	return out
}

func (a *AuctionCellRep) fetchResourcesVia(fetcher func() (executor.ExecutorResources, error)) (auctiontypes.Resources, error) {
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
