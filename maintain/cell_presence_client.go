package maintain

import (
	"time"

	"code.cloudfoundry.org/bbs/models"
	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/consuladapter"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/locket"
	"github.com/tedsuo/ifrit"
)

const CellSchemaKey = "cell"

//go:generate counterfeiter -o fakes/fake_cell_presence_client.go . CellPresenceClient

type CellPresenceClient interface {
	NewCellPresenceRunner(logger lager.Logger, cellPresence *models.CellPresence, retryInterval, lockTTL time.Duration) ifrit.Runner
}

type cellPresenceClient struct {
	consulClient consuladapter.Client
	clock        clock.Clock
}

func CellSchemaPath(cellID string) string {
	return locket.LockSchemaPath(CellSchemaKey, cellID)
}

func NewCellPresenceClient(client consuladapter.Client, clock clock.Clock) *cellPresenceClient {
	return &cellPresenceClient{
		consulClient: client,
		clock:        clock,
	}
}

func (db *cellPresenceClient) NewCellPresenceRunner(logger lager.Logger, cellPresence *models.CellPresence, retryInterval time.Duration, lockTTL time.Duration) ifrit.Runner {
	payload, err := models.ToJSON(cellPresence)
	if err != nil {
		panic(err)
	}

	return locket.NewPresence(logger, db.consulClient, CellSchemaPath(cellPresence.CellId), payload, db.clock, retryInterval, lockTTL)
}
