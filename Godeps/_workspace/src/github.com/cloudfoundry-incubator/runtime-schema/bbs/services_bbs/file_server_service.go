package services_bbs

import (
	"errors"
	"math/rand"
	"time"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/cloudfoundry-incubator/runtime-schema/heartbeater"
	"github.com/cloudfoundry/storeadapter"
	"github.com/tedsuo/ifrit"
)

func (bbs *ServicesBBS) NewFileServerHeartbeat(fileserverURL, fileserverId string, interval time.Duration) ifrit.Runner {
	return heartbeater.New(bbs.store, shared.FileServerSchemaPath(fileserverId), fileserverURL, interval, bbs.logger)
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

func (bbs *ServicesBBS) GetAllFileServers() ([]string, error) {
	node, err := bbs.store.ListRecursively(shared.FileServerSchemaRoot)
	if err == storeadapter.ErrorKeyNotFound {
		return []string{}, nil
	}
	if err != nil {
		return nil, err
	}

	executors := []string{}

	for _, node := range node.ChildNodes {
		executors = append(executors, string(node.Value))
	}

	return executors, nil
}
