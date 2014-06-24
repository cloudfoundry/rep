package maintain

import (
	"os"
	"syscall"
	"time"

	"github.com/cloudfoundry-incubator/executor/client"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	steno "github.com/cloudfoundry/gosteno"
)

type Maintainer struct {
	executorPresence  models.ExecutorPresence
	bbs               Bbs.RepBBS
	executorClient    client.Client
	logger            *steno.Logger
	heartbeatInterval time.Duration
}

func New(executorPresence models.ExecutorPresence, executorClient client.Client, bbs Bbs.RepBBS, logger *steno.Logger, heartbeatInterval time.Duration) *Maintainer {
	return &Maintainer{
		executorPresence:  executorPresence,
		bbs:               bbs,
		executorClient:    executorClient,
		logger:            logger,
		heartbeatInterval: heartbeatInterval,
	}
}

func (m *Maintainer) Run(sigChan <-chan os.Signal, ready chan<- struct{}) error {
	presence, status, err := m.bbs.MaintainExecutorPresence(m.heartbeatInterval, m.executorPresence)
	if err != nil {
		m.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "rep.maintain_presence_begin.failed")
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
				m.logger.Error("rep.maintain_presence.lost-lock")
				continue
			}

			err := m.executorClient.Ping()
			if err != nil {
				m.logger.Errord(map[string]interface{}{
					"error": err.Error(),
				}, "rep.maintain_presence.failed-to-ping-executor")
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
				m.logger.Errord(map[string]interface{}{
					"error": err.Error(),
				}, "rep.maintain_presence_continue.failed")
				status = nil
				continue
			}
			pingTicker.Stop()
			pingTickerChan = nil
			pingTicker = nil
		}
	}
}
