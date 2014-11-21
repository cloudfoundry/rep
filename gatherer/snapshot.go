package gatherer

import (
	"fmt"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

type Snapshot interface {
	// Containers
	ListContainers(tags executor.Tags) []executor.Container // TODO: or pointers?
	GetContainer(guid string) *executor.Container

	// LRP
	ActualLRPs() []models.ActualLRP // TODO: or pointers?

	// Tasks
	Tasks() []models.Task // TODO: or pointers?
	GetTask(guid string) *models.Task
}

type snapshot struct {
	containers []executor.Container
	actualLRPs []models.ActualLRP
	tasks      []models.Task
}

func (s *snapshot) ListContainers(tags executor.Tags) []executor.Container {
	if tags == nil {
		return s.containers
	}

	matched := []executor.Container{}
	for _, c := range s.conatiners {
		if c.HasTags(tags) {
			matched = append(matched, c)
		}
	}

	return matched
}

func (s *snapshot) GetContainer(guid string) *executor.Container {
	for _, c := range s.containers {
		if c.Guid == guid {
			return &c
		}
	}

	return nil
}

func (s *snapshot) ActualLRPs() []models.ActualLRP {
	return s.actualLRPs
}

func (s *snapshot) Tasks() []models.Task {
	return s.tasks
}

func (s *snapshot) GetTask(guid string) *models.Task {
	for _, t := range s.tasks {
		if t.TaskGuid == guid {
			return &t
		}
	}
	return nil
}

func NewSnapshot(bbs bbs.RepBBS, executorClient executor.Client) (Snapshot, error) {
	snap := &snapshot{}
	errChan := make(chan error, 3)

	go func() {
		var err error
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("snapshot-ListContainers failed: %v", r)
			}

			errChan <- err
		}()

		containers, err := executorClient.ListContainers(nil)

		snap.containers = containers
	}()

	go func() {
		var err error
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("snapshot-ActualLRPsByCellID failed: %v", r)
			}

			errChan <- err
		}()

		lrps, err := bbs.ActualLRPsByCellID(g.cellID)

		snap.actualLRPs = lrps
	}()

	go func() {
		var err error
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("snapshot-TasksByCellID failed: %v", r)
			}

			errChan <- err
		}()

		tasks, err := bbs.TasksByCellID(g.cellID)

		snap.tasks = tasks
	}()

	var err error
	for i := 0; i < 3; i++ {
		e := <-errChan
		if err != nil {
			err = e
		}
	}

	return snap, err
}
