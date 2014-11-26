package gatherer

import (
	"fmt"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

type Snapshot interface {
	// Containers
	ListContainers(tags executor.Tags) []executor.Container
	GetContainer(guid string) (*executor.Container, bool)

	// LRP
	ActualLRPs() []models.ActualLRP

	// Tasks
	Tasks() []models.Task
	GetTask(guid string) (*models.Task, bool)
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
	for _, c := range s.containers {
		if c.HasTags(tags) {
			matched = append(matched, c)
		}
	}

	return matched
}

func (s *snapshot) GetContainer(guid string) (*executor.Container, bool) {
	for _, c := range s.containers {
		if c.Guid == guid {
			return &c, true
		}
	}

	return nil, false
}

func (s *snapshot) ActualLRPs() []models.ActualLRP {
	return s.actualLRPs
}

func (s *snapshot) Tasks() []models.Task {
	return s.tasks
}

func (s *snapshot) GetTask(guid string) (*models.Task, bool) {
	for _, t := range s.tasks {
		if t.TaskGuid == guid {
			return &t, true
		}
	}
	return nil, false
}

func NewSnapshot(cellID string, bbs bbs.RepBBS, executorClient executor.Client) (Snapshot, error) {
	snap := &snapshot{}
	errChan := make(chan error, 3)

	go func() {
		containers, err := executorClient.ListContainers(nil)
		if err != nil {
			err = fmt.Errorf("snapshot-ListContainers failed: %s", err.Error())
		}

		snap.containers = containers
		errChan <- err
	}()

	go func() {
		lrps, err := bbs.ActualLRPsByCellID(cellID)
		if err != nil {
			err = fmt.Errorf("snapshot-ActualLRPsByCellID failed: %s", err.Error())
		}

		snap.actualLRPs = lrps
		errChan <- err
	}()

	go func() {
		tasks, err := bbs.TasksByCellID(cellID)
		if err != nil {
			err = fmt.Errorf("snapshot-TasksByCellID failed: %s", err.Error())
		}

		snap.tasks = tasks
		errChan <- err
	}()

	var err error
	for i := 0; i < 3; i++ {
		e := <-errChan
		if err == nil && e != nil {
			err = e
		}
	}

	return snap, err
}
