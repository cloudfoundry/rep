package auction_cell_rep

import (
	"errors"
	"net/url"
	"strconv"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/pivotal-golang/lager"
)

var ErrPreloadedRootFSNotFound = errors.New("preloaded rootfs path not found")

type AuctionCellRep struct {
	cellID               string
	stackPathMap         rep.StackPathMap
	rootFSProviders      auctiontypes.RootFSProviders
	stack                string
	zone                 string
	generateInstanceGuid func() (string, error)
	bbs                  Bbs.RepBBS
	client               executor.Client
	evacuationReporter   evacuation_context.EvacuationReporter
	logger               lager.Logger
}

func New(
	cellID string,
	preloadedStackPathMap rep.StackPathMap,
	arbitraryRootFSes []string,
	zone string,
	generateInstanceGuid func() (string, error),
	bbs Bbs.RepBBS,
	client executor.Client,
	evacuationReporter evacuation_context.EvacuationReporter,
	logger lager.Logger,
) *AuctionCellRep {
	return &AuctionCellRep{
		cellID:               cellID,
		stackPathMap:         preloadedStackPathMap,
		rootFSProviders:      rootFSProviders(preloadedStackPathMap, arbitraryRootFSes),
		zone:                 zone,
		generateInstanceGuid: generateInstanceGuid,
		bbs:                  bbs,
		client:               client,
		evacuationReporter:   evacuationReporter,
		logger:               logger.Session("auction-delegate"),
	}
}

func rootFSProviders(preloaded rep.StackPathMap, arbitrary []string) auctiontypes.RootFSProviders {
	rootFSProviders := auctiontypes.RootFSProviders{}
	for _, scheme := range arbitrary {
		rootFSProviders[scheme] = auctiontypes.ArbitraryRootFSProvider{}
	}

	stacks := make([]string, 0, len(preloaded))
	for stack, _ := range preloaded {
		stacks = append(stacks, stack)
	}
	rootFSProviders["preloaded"] = auctiontypes.NewFixedSetRootFSProvider(stacks...)

	return rootFSProviders
}

func (a *AuctionCellRep) pathForRootFS(rootFS string) (string, bool, error) {
	if rootFS == "" {
		return rootFS, true, nil
	}

	url, err := url.Parse(rootFS)
	if err != nil {
		return "", false, err
	}

	if url.Scheme == models.PreloadedRootFSScheme {
		path, ok := a.stackPathMap[url.Opaque]
		if !ok {
			return "", false, ErrPreloadedRootFSNotFound
		}
		return path, true, nil
	}

	return rootFS, false, nil
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
		RootFSProviders:    a.rootFSProviders,
		AvailableResources: availableResources,
		TotalResources:     totalResources,
		LRPs:               lrps,
		Zone:               a.zone,
		Evacuating:         a.evacuationReporter.Evacuating(),
	}

	a.logger.Info("provided", lager.Data{
		"available-resources": state.AvailableResources,
		"total-resources":     state.TotalResources,
		"num-lrps":            len(state.LRPs),
		"zone":                state.Zone,
		"evacuating":          state.Evacuating,
	})

	return state, nil
}

func (a *AuctionCellRep) Perform(work auctiontypes.Work) (auctiontypes.Work, error) {
	var failedWork = auctiontypes.Work{}

	logger := a.logger.Session("auction-work", lager.Data{
		"lrp-starts": len(work.LRPs),
		"tasks":      len(work.Tasks),
	})

	if a.evacuationReporter.Evacuating() {
		return work, nil
	}

	if len(work.LRPs) > 0 {
		lrpLogger := logger.Session("lrp-allocate-instances")

		containers, lrpAuctionMap, untranslatedLRPs := a.lrpsToContainers(work.LRPs)
		if len(untranslatedLRPs) > 0 {
			lrpLogger.Info("failed-to-translate-lrps-to-containers", lager.Data{"num-failed-to-translate": len(untranslatedLRPs)})
			failedWork.LRPs = untranslatedLRPs
		}

		lrpLogger.Info("requesting-container-allocation", lager.Data{"num-requesting-allocation": len(containers)})
		errMessageMap, err := a.client.AllocateContainers(containers)
		if err != nil {
			lrpLogger.Error("failed-requesting-container-allocation", err)
			failedWork.LRPs = work.LRPs
		} else {
			lrpLogger.Info("succeeded-requesting-container-allocation", lager.Data{"num-failed-to-allocate": len(errMessageMap)})
			for guid, lrpStart := range lrpAuctionMap {
				if _, found := errMessageMap[guid]; found {
					failedWork.LRPs = append(failedWork.LRPs, lrpStart)
				}
			}
		}
	}

	if len(work.Tasks) > 0 {
		taskLogger := logger.Session("task-allocate-instances")

		containers, untranslatedTasks := a.tasksToContainers(work.Tasks)
		if len(untranslatedTasks) > 0 {
			taskLogger.Info("failed-to-translate-tasks-to-containers", lager.Data{"num-failed-to-translate": len(untranslatedTasks)})
			failedWork.Tasks = untranslatedTasks
		}

		taskLogger.Info("requesting-container-allocation", lager.Data{"num-requesting-allocation": len(containers)})
		errMessageMap, err := a.client.AllocateContainers(containers)
		if err != nil {
			taskLogger.Error("failed-requesting-container-allocation", err)
			failedWork.Tasks = work.Tasks
		} else {
			taskLogger.Info("succeeded-requesting-container-allocation", lager.Data{"num-failed-to-allocate": len(errMessageMap)})
			for _, task := range work.Tasks {
				if _, found := errMessageMap[task.TaskGuid]; found {
					failedWork.Tasks = append(failedWork.Tasks, task)
				}
			}
		}
	}

	return failedWork, nil
}

func (a *AuctionCellRep) lrpsToContainers(lrps []auctiontypes.LRPAuction) ([]executor.Container, map[string]auctiontypes.LRPAuction, []auctiontypes.LRPAuction) {
	containers := make([]executor.Container, 0, len(lrps))
	lrpAuctionMap := map[string]auctiontypes.LRPAuction{}
	untranslatedLRPs := make([]auctiontypes.LRPAuction, 0, len(lrps))

	for _, lrpStart := range lrps {
		lrpStart := lrpStart
		instanceGuid, err := a.generateInstanceGuid()
		if err != nil {
			untranslatedLRPs = append(untranslatedLRPs, lrpStart)
			continue
		}

		rootFSPath, preloaded, err := a.pathForRootFS(lrpStart.DesiredLRP.RootFs)
		if err != nil {
			untranslatedLRPs = append(untranslatedLRPs, lrpStart)
			continue
		}

		diskScope := executor.TotalDiskLimit
		if preloaded {
			diskScope = executor.ExclusiveDiskLimit
		}

		containerGuid := rep.LRPContainerGuid(lrpStart.DesiredLRP.ProcessGuid, instanceGuid)
		lrpAuctionMap[containerGuid] = lrpStart

		container := executor.Container{
			Guid: containerGuid,

			Tags: executor.Tags{
				rep.LifecycleTag:    rep.LRPLifecycle,
				rep.DomainTag:       lrpStart.DesiredLRP.Domain,
				rep.ProcessGuidTag:  lrpStart.DesiredLRP.ProcessGuid,
				rep.InstanceGuidTag: instanceGuid,
				rep.ProcessIndexTag: strconv.Itoa(lrpStart.Index),
			},

			MemoryMB:     int(lrpStart.DesiredLRP.MemoryMb),
			DiskMB:       int(lrpStart.DesiredLRP.DiskMb),
			DiskScope:    diskScope,
			CPUWeight:    uint(lrpStart.DesiredLRP.CpuWeight),
			RootFSPath:   rootFSPath,
			Privileged:   lrpStart.DesiredLRP.Privileged,
			Ports:        a.convertPortMappings(lrpStart.DesiredLRP.Ports),
			StartTimeout: uint(lrpStart.DesiredLRP.StartTimeout),

			LogConfig: executor.LogConfig{
				Guid:       lrpStart.DesiredLRP.LogGuid,
				Index:      lrpStart.Index,
				SourceName: lrpStart.DesiredLRP.LogSource,
			},

			MetricsConfig: executor.MetricsConfig{
				Guid:  lrpStart.DesiredLRP.MetricsGuid,
				Index: lrpStart.Index,
			},

			Setup:   lrpStart.DesiredLRP.Setup,
			Action:  lrpStart.DesiredLRP.Action,
			Monitor: lrpStart.DesiredLRP.Monitor,

			Env: append([]executor.EnvironmentVariable{
				{Name: "INSTANCE_GUID", Value: instanceGuid},
				{Name: "INSTANCE_INDEX", Value: strconv.Itoa(lrpStart.Index)},
				{Name: "CF_INSTANCE_GUID", Value: instanceGuid},
				{Name: "CF_INSTANCE_INDEX", Value: strconv.Itoa(lrpStart.Index)},
			}, executor.EnvironmentVariablesFromModel(lrpStart.DesiredLRP.EnvironmentVariables)...),
			EgressRules: lrpStart.DesiredLRP.EgressRules,
		}
		containers = append(containers, container)
	}

	return containers, lrpAuctionMap, untranslatedLRPs
}

func (a *AuctionCellRep) tasksToContainers(tasks []*models.Task) ([]executor.Container, []*models.Task) {
	containers := make([]executor.Container, 0, len(tasks))
	untranslatedTasks := make([]*models.Task, 0, len(tasks))

	for _, task := range tasks {
		rootFSPath, preloaded, err := a.pathForRootFS(task.RootFs)
		if err != nil {
			untranslatedTasks = append(untranslatedTasks, task)
			continue
		}

		diskScope := executor.TotalDiskLimit
		if preloaded {
			diskScope = executor.ExclusiveDiskLimit
		}

		container := executor.Container{
			Guid: task.TaskGuid,

			DiskMB:     int(task.DiskMb),
			DiskScope:  diskScope,
			MemoryMB:   int(task.MemoryMb),
			CPUWeight:  uint(task.CpuWeight),
			RootFSPath: rootFSPath,
			Privileged: task.Privileged,

			LogConfig: executor.LogConfig{
				Guid:       task.LogGuid,
				SourceName: task.LogSource,
			},

			MetricsConfig: executor.MetricsConfig{
				Guid: task.MetricsGuid,
			},

			Tags: executor.Tags{
				rep.LifecycleTag:  rep.TaskLifecycle,
				rep.DomainTag:     task.Domain,
				rep.ResultFileTag: task.ResultFile,
			},

			Action: task.Action,

			Env:         executor.EnvironmentVariablesFromModel(task.EnvironmentVariables),
			EgressRules: task.EgressRules,
		}
		containers = append(containers, container)
	}

	return containers, untranslatedTasks
}

func (a *AuctionCellRep) convertPortMappings(containerPorts []uint32) []executor.PortMapping {
	out := []executor.PortMapping{}
	for _, port := range containerPorts {
		out = append(out, executor.PortMapping{
			ContainerPort: uint16(port),
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
