package gatherer

import (
	"os"
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/timer"
	"github.com/tedsuo/ifrit"
)

type Processor interface {
	Process(snapshot Snapshot)
}

func NewGatherer(
	pollInterval time.Duration,
	timer timer.Timer,
	processors []Processor,
	cellID string,
	bbs bbs.RepBBS,
	executorClient executor.Client,
	logger lager.Logger,
) ifrit.Runner {
	return &gatherer{
		pollInterval:   pollInterval,
		timer:          timer,
		processors:     processors,
		cellID:         cellID,
		bbs:            bbs,
		executorClient: executorClient,
		logger:         logger.Session("gatherer"),
	}
}

type gatherer struct {
	pollInterval time.Duration
	timer        timer.Timer
	processors   []Processor

	cellID         string
	bbs            bbs.RepBBS
	executorClient executor.Client

	logger lager.Logger
}

func (g *gatherer) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	close(ready)

	ticks := g.timer.Every(g.pollInterval)

	for {
		select {
		case <-ticks:
			g.logger.Info("gatherer-entering-loop")

			snapshot, err := NewSnapshot(g.cellID, g.bbs, g.executorClient)
			if err != nil {
				g.logger.Error("failed-to-gather-snapshot", err)
				break
			}

			wg := sync.WaitGroup{}
			wg.Add(len(g.processors))
			for _, p := range g.processors {
				processor := p
				go func() {
					defer wg.Done()
					processor.Process(snapshot)
				}()
			}
			wg.Wait()

			g.logger.Info("gatherer-exiting-loop")
		case <-signals:
			return nil
		}
	}
}
