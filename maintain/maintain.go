package maintain

import (
	"os"
	"syscall"
	"time"

	executorapi "github.com/cloudfoundry-incubator/executor/api"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

type Maintainer struct {
	executorPresence  models.ExecutorPresence
	bbs               Bbs.RepBBS
	executorClient    executorapi.Client
	logger            lager.Logger
	heartbeatInterval time.Duration
}

func New(executorPresence models.ExecutorPresence, executorClient executorapi.Client, bbs Bbs.RepBBS, logger lager.Logger, heartbeatInterval time.Duration) *Maintainer {
	return &Maintainer{
		executorPresence:  executorPresence,
		bbs:               bbs,
		executorClient:    executorClient,
		logger:            logger.Session("maintainer"),
		heartbeatInterval: heartbeatInterval,
	}
}

func (m *Maintainer) Run(sigChan <-chan os.Signal, ready chan<- struct{}) error {
	presence, status, err := m.bbs.MaintainExecutorPresence(m.heartbeatInterval, m.executorPresence)
	if err != nil {
		m.logger.Error("failed-to-start-maintaining-presence", err)
		return err
	}

	if ready != nil {
		close(ready)
	}

	var pingTickerChan <-chan time.Time
	var pingTicker *time.Ticker

	for {
		select {
		case sig := <-sigChan:
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				presence.Remove()
				return nil
			}

		case locked, ok := <-status:
			if !ok {
				return nil
			}

			if !locked {
				m.logger.Error("lost-lock", nil)
				continue
			}

			err := m.executorClient.Ping()
			if err != nil {
				m.logger.Error("failed-to-ping-executor", err)
				status = nil
				presence.Remove()
				pingTicker = time.NewTicker(time.Second)
				pingTickerChan = pingTicker.C
			}

		case <-pingTickerChan:
			err := m.executorClient.Ping()
			if err != nil {
				continue
			}

			presence, status, err = m.bbs.MaintainExecutorPresence(m.heartbeatInterval, m.executorPresence)
			if err != nil {
				m.logger.Error("failed-to-restart-maintaining-presence", err)
				status = nil
				continue
			}

			pingTicker.Stop()
			pingTickerChan = nil
			pingTicker = nil
		}
	}
}
