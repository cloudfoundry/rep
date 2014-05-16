package maintain

import (
	"errors"
	"os"
	"syscall"
	"time"

	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	steno "github.com/cloudfoundry/gosteno"
)

type Maintainer struct {
	repPresence       models.RepPresence
	bbs               Bbs.RepBBS
	logger            *steno.Logger
	heartbeatInterval time.Duration
}

func New(repPresence models.RepPresence, bbs Bbs.RepBBS, logger *steno.Logger, heartbeatInterval time.Duration) *Maintainer {
	return &Maintainer{
		repPresence:       repPresence,
		bbs:               bbs,
		logger:            logger,
		heartbeatInterval: heartbeatInterval,
	}
}

func (m *Maintainer) Run(sigChan chan os.Signal, ready chan struct{}) error {
	for {
		presence, status, err := m.bbs.MaintainRepPresence(m.heartbeatInterval, m.repPresence)
		if err != nil {
			m.logger.Errord(map[string]interface{}{
				"error": err.Error(),
			}, "rep.maintain_presence_begin.failed")
		}

		if ready != nil {
			close(ready)
		}

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
					m.logger.Error("rep.maintain_presence.failed")
					return errors.New("Failed to maintain presence")
				}
			}
		}
	}
}
