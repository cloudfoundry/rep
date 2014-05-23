package lrp_bbs

import (
	"fmt"
	"path"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/storeadapter"
)

type LongRunningProcessBBS struct {
	store storeadapter.StoreAdapter
}

func New(store storeadapter.StoreAdapter) *LongRunningProcessBBS {
	return &LongRunningProcessBBS{
		store: store,
	}
}

func (bbs *LongRunningProcessBBS) DesireLongRunningProcess(lrp models.DesiredLRP) error {
	return shared.RetryIndefinitelyOnStoreTimeout(func() error {
		return bbs.store.SetMulti([]storeadapter.StoreNode{
			{
				Key:   shared.DesiredLRPSchemaPath(lrp),
				Value: lrp.ToJSON(),
			},
		})
	})
}

func (bbs *LongRunningProcessBBS) RemoveActualLongRunningProcess(lrp models.LRP) error {
	return shared.RetryIndefinitelyOnStoreTimeout(func() error {
		return bbs.store.Delete(shared.ActualLRPSchemaPath(lrp))
	})
}

func (bbs *LongRunningProcessBBS) ReportActualLongRunningProcessAsStarting(lrp models.LRP) error {
	lrp.State = models.LRPStateStarting
	return shared.RetryIndefinitelyOnStoreTimeout(func() error {
		return bbs.store.SetMulti([]storeadapter.StoreNode{
			{
				Key:   shared.ActualLRPSchemaPath(lrp),
				Value: lrp.ToJSON(),
			},
		})
	})
}

func (bbs *LongRunningProcessBBS) ReportActualLongRunningProcessAsRunning(lrp models.LRP) error {
	lrp.State = models.LRPStateRunning
	return shared.RetryIndefinitelyOnStoreTimeout(func() error {
		return bbs.store.SetMulti([]storeadapter.StoreNode{
			{
				Key:   shared.ActualLRPSchemaPath(lrp),
				Value: lrp.ToJSON(),
			},
		})
	})
}

func (self *LongRunningProcessBBS) WatchForDesiredLRPChanges() (<-chan models.DesiredLRPChange, chan<- bool, <-chan error) {
	return watchForDesiredLRPChanges(self.store)
}

func (self *LongRunningProcessBBS) WatchForActualLongRunningProcesses() (<-chan models.LRP, chan<- bool, <-chan error) {
	return watchForActualLRPs(self.store)
}

func (bbs *LongRunningProcessBBS) GetAllDesiredLongRunningProcesses() ([]models.DesiredLRP, error) {
	lrps := []models.DesiredLRP{}

	node, err := bbs.store.ListRecursively(shared.DesiredLRPSchemaRoot)
	if err == storeadapter.ErrorKeyNotFound {
		return lrps, nil
	}

	if err != nil {
		return lrps, err
	}

	for _, node := range node.ChildNodes {
		lrp, err := models.NewDesiredLRPFromJSON(node.Value)
		if err != nil {
			return lrps, fmt.Errorf("cannot parse lrp JSON for key %s: %s", node.Key, err.Error())
		} else {
			lrps = append(lrps, lrp)
		}
	}

	return lrps, nil
}

func (bbs *LongRunningProcessBBS) GetAllActualLongRunningProcesses() ([]models.LRP, error) {
	lrps := []models.LRP{}

	node, err := bbs.store.ListRecursively(shared.ActualLRPSchemaRoot)
	if err == storeadapter.ErrorKeyNotFound {
		return lrps, nil
	}

	if err != nil {
		return lrps, err
	}

	for _, node := range node.ChildNodes {
		for _, indexNode := range node.ChildNodes {
			for _, instanceNode := range indexNode.ChildNodes {
				lrp, err := models.NewLRPFromJSON(instanceNode.Value)
				if err != nil {
					return lrps, fmt.Errorf("cannot parse lrp JSON for key %s: %s", instanceNode.Key, err.Error())
				} else {
					lrps = append(lrps, lrp)
				}
			}
		}
	}

	return lrps, nil
}

func (bbs *LongRunningProcessBBS) GetDesiredLRP(processGuid string) (models.DesiredLRP, error) {
	node, err := bbs.store.Get(shared.DesiredLRPSchemaPath(models.DesiredLRP{
		ProcessGuid: processGuid,
	}))
	if err != nil {
		return models.DesiredLRP{}, err
	}
	return models.NewDesiredLRPFromJSON(node.Value)
}

func (bbs *LongRunningProcessBBS) GetActualLRPs(processGuid string) ([]models.LRP, error) {
	lrps := []models.LRP{}

	node, err := bbs.store.ListRecursively(path.Join(shared.ActualLRPSchemaRoot, processGuid))
	if err == storeadapter.ErrorKeyNotFound {
		return lrps, nil
	}

	if err != nil {
		return lrps, err
	}

	for _, indexNode := range node.ChildNodes {
		for _, instanceNode := range indexNode.ChildNodes {
			lrp, err := models.NewLRPFromJSON(instanceNode.Value)
			if err != nil {
				return lrps, fmt.Errorf("cannot parse lrp JSON for key %s: %s", instanceNode.Key, err.Error())
			} else {
				lrps = append(lrps, lrp)
			}
		}
	}

	return lrps, nil
}

func watchForDesiredLRPChanges(store storeadapter.StoreAdapter) (<-chan models.DesiredLRPChange, chan<- bool, <-chan error) {
	changes := make(chan models.DesiredLRPChange)
	stopOuter := make(chan bool)
	errsOuter := make(chan error)

	events, stopInner, errsInner := store.Watch(shared.DesiredLRPSchemaRoot)

	go func() {
		defer close(changes)
		defer close(errsOuter)

		for {
			select {
			case <-stopOuter:
				close(stopInner)
				return

			case event, ok := <-events:
				if !ok {
					return
				}

				var before *models.DesiredLRP
				var after *models.DesiredLRP

				if event.Node != nil {
					aft, err := models.NewDesiredLRPFromJSON(event.Node.Value)
					if err != nil {
						continue
					}

					after = &aft
				}

				if event.PrevNode != nil {
					bef, err := models.NewDesiredLRPFromJSON(event.PrevNode.Value)
					if err != nil {
						continue
					}

					before = &bef
				}

				changes <- models.DesiredLRPChange{
					Before: before,
					After:  after,
				}

			case err, ok := <-errsInner:
				if ok {
					errsOuter <- err
				}
				return
			}
		}
	}()

	return changes, stopOuter, errsOuter
}

func watchForActualLRPs(store storeadapter.StoreAdapter) (<-chan models.LRP, chan<- bool, <-chan error) {
	lrps := make(chan models.LRP)
	stopOuter := make(chan bool)
	errsOuter := make(chan error)

	events, stopInner, errsInner := store.Watch(shared.ActualLRPSchemaRoot)

	go func() {
		defer close(lrps)
		defer close(errsOuter)

		for {
			select {
			case <-stopOuter:
				close(stopInner)
				return

			case event, ok := <-events:
				if !ok {
					return
				}

				lrp, err := models.NewLRPFromJSON(event.Node.Value)
				if err != nil {
					continue
				}

				lrps <- lrp

			case err, ok := <-errsInner:
				if ok {
					errsOuter <- err
				}
				return
			}
		}
	}()

	return lrps, stopOuter, errsOuter
}
