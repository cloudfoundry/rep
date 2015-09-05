package rep

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/cloudfoundry-incubator/bbs/models"
)

var ErrorIncompatibleRootfs = errors.New("rootfs not found")
var ErrorInsufficientResources = errors.New("insufficient resources")

type CellState struct {
	RootFSProviders    RootFSProviders
	AvailableResources Resources
	TotalResources     Resources
	LRPs               []LRP
	Tasks              []Task
	Zone               string
	Evacuating         bool
}

func NewCellState(root RootFSProviders, avail Resources, total Resources, lrps []LRP, tasks []Task, zone string, isEvac bool) CellState {
	return CellState{root, avail, total, lrps, tasks, zone, isEvac}
}

func (c *CellState) Copy() CellState {
	lrps := make([]LRP, 0, len(c.LRPs))
	copy(lrps, c.LRPs)
	tasks := make([]Task, 0, len(c.Tasks))
	copy(tasks, c.Tasks)
	return NewCellState(c.RootFSProviders.Copy(), c.AvailableResources, c.TotalResources, lrps, tasks, c.Zone, c.Evacuating)
}

func (c *CellState) AddLRP(lrp *LRP) {
	c.AvailableResources.Subtract(&lrp.Resource)
	c.LRPs = append(c.LRPs, *lrp)
}

func (c *CellState) AddTask(task *Task) {
	c.AvailableResources.Subtract(&task.Resource)
	c.Tasks = append(c.Tasks, *task)
}

func (c *CellState) ResourceMatch(res *Resource) error {
	switch {
	case !c.MatchRootFS(res.RootFs):
		return ErrorIncompatibleRootfs
	case c.AvailableResources.MemoryMB < res.MemoryMB:
		return ErrorInsufficientResources
	case c.AvailableResources.DiskMB < res.DiskMB:
		return ErrorInsufficientResources
	case c.AvailableResources.Containers < 1:
		return ErrorInsufficientResources
	default:
		return nil
	}
}

func (c CellState) ComputeScore(res *Resource) float64 {
	remainingResources := c.AvailableResources.Copy()
	remainingResources.Subtract(res)
	return remainingResources.ComputeScore(&c.TotalResources)
}

func (c *CellState) MatchRootFS(rootfs string) bool {
	rootFSURL, err := url.Parse(rootfs)
	if err != nil {
		return false
	}

	return c.RootFSProviders.Match(*rootFSURL)
}

type Resources struct {
	MemoryMB   int32
	DiskMB     int32
	Containers int
}

func NewResources(memoryMb, diskMb int32, containerCount int) Resources {
	return Resources{memoryMb, diskMb, containerCount}
}

func (r *Resources) Copy() Resources {
	return *r
}

func (r *Resources) Subtract(res *Resource) {
	r.MemoryMB -= res.MemoryMB
	r.DiskMB -= res.DiskMB
	r.Containers -= 1
}

func (r *Resources) ComputeScore(total *Resources) float64 {
	fractionUsedMemory := 1.0 - float64(r.MemoryMB)/float64(total.MemoryMB)
	fractionUsedDisk := 1.0 - float64(r.DiskMB)/float64(total.DiskMB)
	fractionUsedContainers := 1.0 - float64(r.Containers)/float64(total.Containers)
	return (fractionUsedMemory + fractionUsedDisk + fractionUsedContainers) / 3.0
}

type Resource struct {
	MemoryMB int32
	DiskMB   int32
	RootFs   string
}

func NewResource(memoryMb, diskMb int32, rootfs string) Resource {
	return Resource{memoryMb, diskMb, rootfs}
}

func (r *Resource) Empty() bool {
	return r.DiskMB == 0 && r.MemoryMB == 0 && r.RootFs == ""
}

func (r *Resource) Copy() Resource {
	return NewResource(r.MemoryMB, r.DiskMB, r.RootFs)
}

type LRP struct {
	models.ActualLRPKey
	Resource
}

func NewLRP(key models.ActualLRPKey, res Resource) LRP {
	return LRP{key, res}
}

func (lrp *LRP) Identifier() string {
	return fmt.Sprintf("%s.%d", lrp.ProcessGuid, lrp.Index)
}

func (lrp *LRP) Copy() LRP {
	return NewLRP(lrp.ActualLRPKey, lrp.Resource)
}

type Task struct {
	TaskGuid string
	Resource
}

func NewTask(guid string, res Resource) Task {
	return Task{guid, res}
}

func (task *Task) Identifier() string {
	return task.TaskGuid
}

func (task *Task) Copy() Task {
	return NewTask(task.TaskGuid, task.Resource)
}

type Work struct {
	LRPs  []LRP
	Tasks []Task
}
