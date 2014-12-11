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
	cellID string
	stack  string
	bbs    Bbs.RepBBS
	client executor.Client
	logger lager.Logger
}

func New(cellID string, stack string, bbs Bbs.RepBBS, client executor.Client, logger lager.Logger) *AuctionCellRep {
	return &AuctionCellRep{
		cellID: cellID,
		stack:  stack,
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
		"lrp-starts": len(work.LRPStarts),
		"tasks":      len(work.Tasks),
	})

	for _, start := range work.LRPStarts {
		startLogger := logger.Session("lrp-start-instance", lager.Data{
			"process-guid":  start.DesiredLRP.ProcessGuid,
			"instance-guid": start.InstanceGuid,
			"index":         start.Index,
			"memory-mb":     start.DesiredLRP.MemoryMB,
			"disk-mb":       start.DesiredLRP.DiskMB,
		})
		startLogger.Info("starting")
		err := a.startLRP(start, startLogger)
		if err != nil {
			startLogger.Error("failed-to-start", err)
			failedWork.LRPStarts = append(failedWork.LRPStarts, start)
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

func (a *AuctionCellRep) startLRP(startAuction models.LRPStartAuction, logger lager.Logger) error {

	containerGuid := startAuction.InstanceGuid

	logger.Info("reserving")
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
		logger.Error("failed-reserving", err)
		return err
	}
	logger.Info("succeeded-reserving")

	go func() {
		claiming := models.NewActualLRP(
			startAuction.DesiredLRP.ProcessGuid,
			startAuction.InstanceGuid,
			a.cellID,
			startAuction.DesiredLRP.Domain,
			startAuction.Index,
			"",
		)

		logger.Info("announcing-to-bbs")
		_, claimErr := a.bbs.ClaimActualLRP(claiming)
		if claimErr != nil {
			logger.Error("failed-announcing-to-bbs", claimErr)
			a.client.DeleteContainer(containerGuid)
			return
		}
		logger.Info("succeeded-announcing-to-bbs")

		logger.Info("running-container")
		runErr := a.client.RunContainer(containerGuid)
		if runErr != nil {
			logger.Error("failed-running-container", runErr)
			a.client.DeleteContainer(containerGuid)
			a.bbs.RemoveActualLRP(claiming)
		}
		logger.Info("succeeded-running-container")
	}()

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

	go func() {
		logger.Info("starting-task")
		err = a.bbs.StartTask(task.TaskGuid, a.cellID)
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
	}()

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
	logger.Info("complete-task")
	err = a.bbs.CompleteTask(taskGuid, true, "failed to run container - "+err.Error(), "")
	if err != nil {
		logger.Error("failed-to-complete-task", err)
	}
	logger.Info("successfully-completed-task")
}
