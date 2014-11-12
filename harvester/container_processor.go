package harvester

import (
	"fmt"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/pivotal-golang/lager"
)

type Processor interface {
	Process(executor.Container)
}

type containerProcessor struct {
	taskProcessor Processor
	lrpProcessor  Processor
	logger        lager.Logger
}

func NewContainerProcessor(
	logger lager.Logger,
	taskProcessor Processor,
	lrpProcessor Processor,
) Processor {
	return &containerProcessor{
		taskProcessor: taskProcessor,
		lrpProcessor:  lrpProcessor,
		logger:        logger.Session("container-processor"),
	}
}

func (p *containerProcessor) Process(container executor.Container) {
	logger := p.logger.WithData(lager.Data{
		"container-guid":   container.Guid,
		"container-state":  container.State,
		"container-health": container.Health,
	})

	if container.Tags == nil {
		logger.Debug("container-missing-tags")
		return
	}

	logger.Debug("container-tags", lager.Data{"tags": fmt.Sprintf("%#v", container.Tags)})

	lifecycle, found := container.Tags[rep.LifecycleTag]
	if !found {
		logger.Debug("ignoring-container-without-lifecycle-tag")
		return
	}

	switch lifecycle {
	case rep.TaskLifecycle:
		p.taskProcessor.Process(container)

	case rep.LRPLifecycle:
		p.lrpProcessor.Process(container)

	default:
		logger.Debug("ignoring-unknown-lifecycle-tag", lager.Data{"container-lifecycle": lifecycle})
	}
}
