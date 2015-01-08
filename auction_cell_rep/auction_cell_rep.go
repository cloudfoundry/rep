package auction_cell_rep

import (
	"errors"
	"fmt"
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

	for _, start := range work.LRPs {
		startLogger := logger.Session("lrp-start-instance", lager.Data{
			"process-guid": start.DesiredLRP.ProcessGuid,
			"index":        start.Index,
			"memory-mb":    start.DesiredLRP.MemoryMB,
			"disk-mb":      start.DesiredLRP.DiskMB,
		})
		startLogger.Info("starting")
		err := a.startLRP(start, startLogger)
		if err != nil {
			startLogger.Error("failed-to-start", err)
			failedWork.LRPs = append(failedWork.LRPs, start)
		} else {
			startLogger.Info("started")
		}
	}

	for _, task := range work.Tasks {
		taskLogger := logger.Session("task-start", lager.Data{
			"task-guid": task.TaskGuid,
			"memory-mb": task.MemoryMB,
			"disk-mb":   task.DiskMB,
		})
		taskLogger.Info("starting")
		err := a.startTask(task, taskLogger)
		if err != nil {
			taskLogger.Error("failed-to-start", err)
			failedWork.Tasks = append(failedWork.Tasks, task)
		} else {
			taskLogger.Info("started")
		}
	}

	return failedWork, nil
}

func (a *AuctionCellRep) startLRP(lrpStart auctiontypes.LRPAuction, logger lager.Logger) error {

	containerGuidString, err := a.generateContainerGuid()
	if err != nil {
		logger.Error("generating-instance-guid-failed", err)
		return err
	}

	logger = logger.WithData(lager.Data{"instance-guid": containerGuidString})
	logger.Info("reserving")
	_, err = a.client.AllocateContainer(executor.Container{
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
		Privileged:   lrpStart.DesiredLRP.Privileged,
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
	})

	if err != nil {
		logger.Error("failed-reserving", err)
		return err
	}

	logger.Info("succeeded-reserving")

	return nil
}

func (a *AuctionCellRep) startTask(task models.Task, logger lager.Logger) error {
	if task.Stack != a.stack {
		return errors.New(fmt.Sprintf("stack mismatch: task requested stack '%s', rep provides stack '%s'", task.Stack, a.stack))
	}

	logger.Info("allocating-container")
	_, err := a.client.AllocateContainer(executor.Container{
		Guid: task.TaskGuid,

		Tags: executor.Tags{
			rep.LifecycleTag:  rep.TaskLifecycle,
			rep.DomainTag:     task.Domain,
			rep.ResultFileTag: task.ResultFile,
		},

		DiskMB:     task.DiskMB,
		MemoryMB:   task.MemoryMB,
		CPUWeight:  task.CPUWeight,
		RootFSPath: task.RootFSPath,
		Privileged: task.Privileged,
		Log: executor.LogConfig{
			Guid:       task.LogGuid,
			SourceName: task.LogSource,
		},

		Action: task.Action,

		Env: executor.EnvironmentVariablesFromModel(task.EnvironmentVariables),
	})

	if err != nil {
		logger.Error("failed-to-allocate-container", err)
		return err
	}

	logger.Info("successfully-allocated-container")
	return nil
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
