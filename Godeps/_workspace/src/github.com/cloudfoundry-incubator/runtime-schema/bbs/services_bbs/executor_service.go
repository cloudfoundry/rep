package services_bbs

import (
	"time"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/storeadapter"
)

func (bbs *ServicesBBS) MaintainExecutorPresence(heartbeatInterval time.Duration, executorPresence models.ExecutorPresence) (Presence, <-chan bool, error) {
	presence := NewPresence(bbs.store, shared.ExecutorSchemaPath(executorPresence.ExecutorID), executorPresence.ToJSON())
	status, err := presence.Maintain(heartbeatInterval)
	return presence, status, err
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
			bbs.logger.Errord(map[string]interface{}{
				"error": err.Error(),
			}, "bbs.get-all-executors.invalid-json")
			continue
		}
		executorPresences = append(executorPresences, executorPresence)
	}
	return executorPresences, nil
}
