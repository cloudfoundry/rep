package fake_bbs

import "github.com/cloudfoundry-incubator/runtime-schema/models"

type FakeLRPRouterBBS struct {
	DesiredLRPChangeChan chan models.DesiredLRPChange
	desiredLRPStopChan   chan bool
	desiredLRPErrChan    chan error

	ActualLRPChan     chan models.LRP
	actualLRPStopChan chan bool
	actualLRPErrChan  chan error

	AllDesiredLRPs []models.DesiredLRP
	AllActualLRPs  []models.LRP

	DesiredLRP models.DesiredLRP
	ActualLRPs []models.LRP

	WhenGettingAllActualLongRunningProcesses  func() ([]models.LRP, error)
	WhenGettingAllDesiredLongRunningProcesses func() ([]models.DesiredLRP, error)
}

func NewFakeLRPRouterBBS() *FakeLRPRouterBBS {
	return &FakeLRPRouterBBS{
		DesiredLRPChangeChan: make(chan models.DesiredLRPChange, 1),
		desiredLRPStopChan:   make(chan bool),
		desiredLRPErrChan:    make(chan error),

		ActualLRPChan:     make(chan models.LRP, 1),
		actualLRPStopChan: make(chan bool),
		actualLRPErrChan:  make(chan error),
	}
}

func (fakeBBS *FakeLRPRouterBBS) WatchForDesiredLRPChanges() (<-chan models.DesiredLRPChange, chan<- bool, <-chan error) {
	return fakeBBS.DesiredLRPChangeChan, fakeBBS.desiredLRPStopChan, fakeBBS.desiredLRPErrChan
}

func (fakeBBS *FakeLRPRouterBBS) WatchForActualLongRunningProcesses() (<-chan models.LRP, chan<- bool, <-chan error) {
	return fakeBBS.ActualLRPChan, fakeBBS.actualLRPStopChan, fakeBBS.actualLRPErrChan
}

func (fakeBBS *FakeLRPRouterBBS) GetAllDesiredLongRunningProcesses() ([]models.DesiredLRP, error) {
	if fakeBBS.WhenGettingAllDesiredLongRunningProcesses != nil {
		return fakeBBS.WhenGettingAllDesiredLongRunningProcesses()
	}
	return fakeBBS.AllDesiredLRPs, nil
}

func (fakeBBS *FakeLRPRouterBBS) GetAllActualLongRunningProcesses() ([]models.LRP, error) {
	if fakeBBS.WhenGettingAllActualLongRunningProcesses != nil {
		return fakeBBS.WhenGettingAllActualLongRunningProcesses()
	}
	return fakeBBS.AllActualLRPs, nil
}

func (fakeBBS *FakeLRPRouterBBS) GetDesiredLRP(processGuid string) (models.DesiredLRP, error) {
	return fakeBBS.DesiredLRP, nil
}

func (fakeBBS *FakeLRPRouterBBS) GetActualLRPs(processGuid string) ([]models.LRP, error) {
	return fakeBBS.ActualLRPs, nil
}
