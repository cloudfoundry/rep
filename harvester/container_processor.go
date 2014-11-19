package harvester

import (
	"sync"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry/gunk/workpool"
	"github.com/pivotal-golang/lager"
)

type Processor interface {
	Process(executor.Container)
}

type ProcessorQueue interface {
	WorkPending() bool
}

type containerProcessor struct {
	taskProcessor Processor
	lrpProcessor  Processor
	logger        lager.Logger
	mutex         *sync.Mutex
	cond          *sync.Cond
	pending       map[string]executor.Container
	inFlight      map[string]struct{}
	inFlightQ     []string
	workQ         *workpool.WorkPool
}

func NewContainerProcessor(
	logger lager.Logger,
	taskProcessor Processor,
	lrpProcessor Processor,
) Processor {
	workq := workpool.NewWorkPool(16)
	mutex := &sync.Mutex{}
	cond := sync.NewCond(mutex)

	cp := &containerProcessor{
		taskProcessor: taskProcessor,
		lrpProcessor:  lrpProcessor,
		logger:        logger.Session("container-processor"),
		mutex:         mutex,
		cond:          cond,
		pending:       map[string]executor.Container{},
		inFlight:      map[string]struct{}{},
		inFlightQ:     []string{},
		workQ:         workq,
	}

	for i := 0; i < 16; i++ {
		workq.Submit(cp.processWorkQueue)
	}

	return cp
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

	p.mutex.Lock()
	p.pending[container.Guid] = container
	p.enqueue(container.Guid)
	p.mutex.Unlock()
}

func (p *containerProcessor) enqueue(guid string) {
	_, exists := p.inFlight[guid]
	if !exists {
		p.inFlight[guid] = struct{}{}
		p.inFlightQ = append(p.inFlightQ, guid)
		p.cond.Signal()
	}
}

func (p *containerProcessor) processWorkQueue() {
	logger := p.logger.Session("process-work-queue")

	for {
		p.mutex.Lock()
		for len(p.inFlightQ) == 0 {
			p.cond.Wait()
		}

		containerGuid := p.inFlightQ[0]
		p.inFlightQ = p.inFlightQ[1:]

		container := p.pending[containerGuid]
		delete(p.pending, container.Guid)
		p.mutex.Unlock()

		lifecycle := container.Tags[rep.LifecycleTag]
		logger.Info("processing-container", lager.Data{
			"container-guid":      container.Guid,
			"container-state":     container.State,
			"container-health":    container.Health,
			"container-lifecycle": lifecycle,
		})

		switch lifecycle {
		case rep.TaskLifecycle:
			p.taskProcessor.Process(container)

		case rep.LRPLifecycle:
			p.lrpProcessor.Process(container)

		default:
			logger.Debug("ignoring-unknown-lifecycle-tag", lager.Data{
				"container-lifecycle": lifecycle,
			})
		}

		p.mutex.Lock()
		delete(p.inFlight, container.Guid)
		if _, exists := p.pending[container.Guid]; exists {
			p.enqueue(container.Guid)
		}
		p.mutex.Unlock()
	}
}

func (p *containerProcessor) WorkPending() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if len(p.pending) > 0 {
		return true
	}

	if len(p.inFlight) > 0 {
		return true
	}

	if len(p.inFlightQ) > 0 {
		return true
	}

	return false
}
