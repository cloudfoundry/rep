package auction_cell_rep

import (
	"strconv"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

type AuctionCellRep struct {
	cellID                string
	stack                 string
	zone                  string
	generateContainerGuid func() (string, error)
	bbs                   Bbs.RepBBS
	client                executor.Client
	logger                lager.Logger
}

func New(cellID string, stack string, zone string, generateContainerGuid func() (string, error), bbs Bbs.RepBBS, client executor.Client, logger lager.Logger) *AuctionCellRep {
	return &AuctionCellRep{
		cellID: cellID,
		stack:  stack,
		zone:   zone,
		generateContainerGuid: generateContainerGuid,
		bbs:    bbs,
		client: client,
		logger: logger.Session("auction-delegate"),
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
			ProcessGuid: container.Tags[rep.ProcessGuidTag],
			Index:       index,
			MemoryMB:    container.MemoryMB,
			DiskMB:      container.DiskMB,
		}
		lrps = append(lrps, lrp)
	}

	state := auctiontypes.CellState{
		Stack:              a.stack,
		AvailableResources: availableResources,
		TotalResources:     totalResources,
		LRPs:               lrps,
		Zone:               a.zone,
	}

	a.logger.Session("provided", lager.Data{"state": state})

	return state, nil
}

func (a *AuctionCellRep) Perform(work auctiontypes.Work) (auctiontypes.Work, error) {
	var failedWork = auctiontypes.Work{}

	logger := a.logger.Session("auction-work", lager.Data{
		"lrp-starts": len(work.LRPs),
		"tasks":      len(work.Tasks),
	})

	containers, lrpAuctionMap, err := a.lrpsToContainers(work.LRPs)
	if err != nil {
		failedWork.LRPs = work.LRPs
	} else {
		errMessageMap, err := a.client.AllocateContainers(containers)
		if err != nil {
			failedWork.LRPs = work.LRPs
		} else {
			for guid, lrpStart := range lrpAuctionMap {
				if _, found := errMessageMap[guid]; found {
					failedWork.LRPs = append(failedWork.LRPs, lrpStart)
				} else {
					go a.startLRP(guid, lrpStart, logger)
				}
			}
		}
	}

	containers = a.tasksToContainers(work.Tasks)
	errMessageMap, err := a.client.AllocateContainers(containers)
	if err != nil {
		failedWork.Tasks = work.Tasks
	} else {
		for _, task := range work.Tasks {
			if _, found := errMessageMap[task.TaskGuid]; found {
				failedWork.Tasks = append(failedWork.Tasks, task)
			} else {
				go a.startTask(task, logger)
			}
		}
	}

	return failedWork, nil
}

func (a *AuctionCellRep) lrpsToContainers(lrps []auctiontypes.LRPAuction) ([]executor.Container, map[string]auctiontypes.LRPAuction, error) {
	containers := make([]executor.Container, 0, len(lrps))
	lrpAuctionMap := map[string]auctiontypes.LRPAuction{}

	for _, lrpStart := range lrps {
		containerGuidString, err := a.generateContainerGuid()
		if err != nil {
			return nil, nil, err
		}
		lrpAuctionMap[containerGuidString] = lrpStart

		container := executor.Container{
			Guid: containerGuidString,

			Tags: executor.Tags{
				rep.LifecycleTag:    rep.LRPLifecycle,
				rep.DomainTag:       lrpStart.DesiredLRP.Domain,
				rep.ProcessGuidTag:  lrpStart.DesiredLRP.ProcessGuid,
				rep.ProcessIndexTag: strconv.Itoa(lrpStart.Index),
			},

			MemoryMB:     lrpStart.DesiredLRP.MemoryMB,
			DiskMB:       lrpStart.DesiredLRP.DiskMB,
			CPUWeight:    lrpStart.DesiredLRP.CPUWeight,
			RootFSPath:   lrpStart.DesiredLRP.RootFSPath,
			Ports:        a.convertPortMappings(lrpStart.DesiredLRP.Ports),
			StartTimeout: lrpStart.DesiredLRP.StartTimeout,

			Log: executor.LogConfig{
				Guid:       lrpStart.DesiredLRP.LogGuid,
				SourceName: lrpStart.DesiredLRP.LogSource,
				Index:      &lrpStart.Index,
			},

			Setup:   lrpStart.DesiredLRP.Setup,
			Action:  lrpStart.DesiredLRP.Action,
			Monitor: lrpStart.DesiredLRP.Monitor,

			Env: append([]executor.EnvironmentVariable{
				{Name: "INSTANCE_GUID", Value: containerGuidString},
				{Name: "INSTANCE_INDEX", Value: strconv.Itoa(lrpStart.Index)},
			}, executor.EnvironmentVariablesFromModel(lrpStart.DesiredLRP.EnvironmentVariables)...),
		}
		containers = append(containers, container)
	}

	return containers, lrpAuctionMap, nil
}

func (a *AuctionCellRep) startLRP(guid string, lrpStart auctiontypes.LRPAuction, logger lager.Logger) {
	lrpKey := models.NewActualLRPKey(
		lrpStart.DesiredLRP.ProcessGuid,
		lrpStart.Index,
		lrpStart.DesiredLRP.Domain,
	)
	lrpContainerKey := models.NewActualLRPContainerKey(
		guid,
		a.cellID,
	)

	logger.Info("announcing-to-bbs")
	claimErr := a.bbs.ClaimActualLRP(lrpKey, lrpContainerKey, logger)
	if claimErr != nil {
		logger.Error("failed-announcing-to-bbs", claimErr)
		a.client.DeleteContainer(guid)
		return
	}
	logger.Info("succeeded-announcing-to-bbs")

	logger.Info("running-container")
	runErr := a.client.RunContainer(guid)
	if runErr != nil {
		logger.Error("failed-running-container", runErr)
		a.client.DeleteContainer(guid)
		a.bbs.RemoveActualLRP(lrpKey, lrpContainerKey, logger)
		return
	}
	logger.Info("succeeded-running-container")
}

func (a *AuctionCellRep) tasksToContainers(tasks []models.Task) []executor.Container {
	containers := make([]executor.Container, 0, len(tasks))

	for _, task := range tasks {
		container := executor.Container{
			Guid: task.TaskGuid,

			DiskMB:     task.DiskMB,
			MemoryMB:   task.MemoryMB,
			CPUWeight:  task.CPUWeight,
			RootFSPath: task.RootFSPath,
			Privileged: task.Privileged,
			Log: executor.LogConfig{
				Guid:       task.LogGuid,
				SourceName: task.LogSource,
			},
			Tags: executor.Tags{
				rep.LifecycleTag:  rep.TaskLifecycle,
				rep.DomainTag:     task.Domain,
				rep.ResultFileTag: task.ResultFile,
			},

			Action: task.Action,

			Env: executor.EnvironmentVariablesFromModel(task.EnvironmentVariables),
		}
		containers = append(containers, container)
	}

	return containers
}

func (a *AuctionCellRep) startTask(task models.Task, logger lager.Logger) {
	logger.Info("starting-task")
	err := a.bbs.StartTask(logger, task.TaskGuid, a.cellID)
	if err != nil {
		logger.Error("failed-to-mark-task-started", err)
		a.client.DeleteContainer(task.TaskGuid)
		return
	}
	logger.Info("successfully-started-task")

	logger.Info("running-task")
	err = a.client.RunContainer(task.TaskGuid)
	if err != nil {
		logger.Error("failed-to-run-task", err)
		a.client.DeleteContainer(task.TaskGuid)
		a.markTaskAsFailed(logger, task.TaskGuid, err)
		return
	}
	logger.Info("successfully-ran-task")
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

func (a *AuctionCellRep) markTaskAsFailed(logger lager.Logger, taskGuid string, err error) {
	logger.Info("fail-task")
	err = a.bbs.FailTask(logger, taskGuid, "failed to run container - "+err.Error())
	if err != nil {
		logger.Error("failed-to-fail-task", err)
	}
	logger.Info("successfully-failed-task")
}
