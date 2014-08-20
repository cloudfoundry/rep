package services_bbs

import (
	"time"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/cloudfoundry-incubator/runtime-schema/heartbeater"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/storeadapter"
	"github.com/tedsuo/ifrit"
)

func (bbs *ServicesBBS) NewExecutorHeartbeat(executorPresence models.ExecutorPresence, interval time.Duration) ifrit.Runner {
	return heartbeater.New(bbs.store, shared.ExecutorSchemaPath(executorPresence.ExecutorID), string(executorPresence.ToJSON()), interval, bbs.logger)
}

func (bbs *ServicesBBS) GetAllExecutors() ([]models.ExecutorPresence, error) {
	node, err := bbs.store.ListRecursively(shared.ExecutorSchemaRoot)
	if err == storeadapter.ErrorKeyNotFound {
		return []models.ExecutorPresence{}, nil
	}

	if err != nil {
		return nil, err
	}

	var executorPresences []models.ExecutorPresence
	for _, node := range node.ChildNodes {
		executorPresence, err := models.NewExecutorPresenceFromJSON(node.Value)
		if err != nil {
			bbs.logger.Error("failed-to-unmarshal-executors-json", err)
			continue
		}

		executorPresences = append(executorPresences, executorPresence)
	}

	return executorPresences, nil
}
