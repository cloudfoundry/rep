package maintain

import (
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/pivotal-golang/clock"
	"github.com/pivotal-golang/lager"
	"github.com/tedsuo/ifrit"
)

type Maintainer struct {
	heartbeater       ifrit.Runner
	executorClient    executor.Client
	logger            lager.Logger
	heartbeatInterval time.Duration
	clock             clock.Clock
}

func New(
	executorClient executor.Client,
	heartbeater ifrit.Runner,
	logger lager.Logger,
	heartbeatInterval time.Duration,
	clock clock.Clock,
) *Maintainer {
	return &Maintainer{
		heartbeater:       heartbeater,
		executorClient:    executorClient,
		logger:            logger.Session("maintainer"),
		heartbeatInterval: heartbeatInterval,
		clock:             clock,
	}
}

func (m *Maintainer) Run(sigChan <-chan os.Signal, ready chan<- struct{}) error {
	for {
		err := m.executorClient.Ping()
		if err == nil {
			break
		}

		m.logger.Error("failed-to-ping-executor-on-start", err)
		m.clock.Sleep(time.Second)
	}

	heartbeatProcess := ifrit.Invoke(m.heartbeater)
	heartbeatExitChan := heartbeatProcess.Wait()

	close(ready)

	ticker := m.clock.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case err := <-heartbeatExitChan:
			m.logger.Error("lost-lock", err)
			heartbeatExitChan = nil

		case <-sigChan:
			heartbeatProcess.Signal(os.Kill)
			<-heartbeatProcess.Wait()
			return nil

		case <-ticker.C():
			err := m.executorClient.Ping()
			if err != nil {
				heartbeatProcess.Signal(os.Kill)
				heartbeatExitChan = nil
			} else if heartbeatExitChan == nil {
				heartbeatProcess = ifrit.Invoke(m.heartbeater)
				heartbeatExitChan = heartbeatProcess.Wait()
			}

			if err != nil {
				m.logger.Error("failed-to-restart-maintaining-presence", err)
				continue
			}
		}
	}
}
