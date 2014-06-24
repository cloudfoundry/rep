package services_bbs

import (
	"errors"
	"math/rand"
	"time"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
)

func (bbs *ServicesBBS) MaintainFileServerPresence(heartbeatInterval time.Duration, fileServerURL string, fileServerId string) (Presence, <-chan bool, error) {
	key := shared.FileServerSchemaPath(fileServerId)
	presence := NewPresence(bbs.store, key, []byte(fileServerURL))
	status, err := presence.Maintain(heartbeatInterval)
	return presence, status, err
}

func (bbs *ServicesBBS) GetAvailableFileServer() (string, error) {
	node, err := bbs.store.ListRecursively(shared.FileServerSchemaRoot)
	if err != nil {
		return "", err
	}

	if len(node.ChildNodes) == 0 {
		return "", errors.New("No file servers are currently available")
	}

	randomServerIndex := rand.Intn(len(node.ChildNodes))
	return string(node.ChildNodes[randomServerIndex].Value), nil
}
