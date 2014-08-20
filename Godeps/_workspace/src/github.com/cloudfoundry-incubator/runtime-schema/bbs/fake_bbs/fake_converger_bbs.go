package fake_bbs

import (
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/fake_runner"
)

type FakeConvergerBBS struct {
	callsToConvergeTasks            int
	callsToConvergeLRPs             int
	callsToConvergeLRPStartAuctions int
	callsToConvergeLRPStopAuctions  int

	ConvergeLockStatusChan chan bool
	ConvergeLockStopChan   chan chan bool

	maintainConvergeInterval      time.Duration
	maintainConvergeExecutorID    string
	maintainConvergeStatusChannel <-chan bool
	maintainConvergeStopChannel   chan<- chan bool
	maintainConvergeLockError     error

	convergeTimeToClaimTasks time.Duration
	taskConvergenceInterval  time.Duration

	FileServerGetter

	DesiredLRPChangeChan chan models.DesiredLRPChange
	DesiredLRPStopChan   chan bool
	DesiredLRPErrChan    chan error

	stopLRPInstances   []models.StopLRPInstance
	StopLRPInstanceErr error

	desiredLRPs  []models.DesiredLRP
	DesireLRPErr error

	removeDesiredLRPProcessGuids    []string
	removeDesiredLRPProcessGuidsErr error

	ActualLRPs    []models.ActualLRP
	ActualLRPsErr error

	lrpStartAuctions               []models.LRPStartAuction
	LRPStartAuctionErr             error
	WhenRequestingLRPStartAuctions func(lrp models.LRPStartAuction) error

	lrpStopAuctions               []models.LRPStopAuction
	LRPStopAuctionErr             error
	WhenRequestingLRPStopAuctions func(lrp models.LRPStopAuction) error

	sync.RWMutex
}

func NewFakeConvergerBBS() *FakeConvergerBBS {
	fakeBBS := &FakeConvergerBBS{
		ConvergeLockStatusChan: make(chan bool),
		ConvergeLockStopChan:   make(chan chan bool),

		DesiredLRPChangeChan: make(chan models.DesiredLRPChange, 1),
		DesiredLRPStopChan:   make(chan bool),
		DesiredLRPErrChan:    make(chan error),

		WhenRequestingLRPStartAuctions: nil,
		WhenRequestingLRPStopAuctions:  nil,
	}
	return fakeBBS
}

func (fakeBBS *FakeConvergerBBS) ConvergeLRPs() {
	fakeBBS.Lock()
	defer fakeBBS.Unlock()

	fakeBBS.callsToConvergeLRPs++
}

func (fakeBBS *FakeConvergerBBS) CallsToConvergeLRPs() int {
	fakeBBS.RLock()
	defer fakeBBS.RUnlock()

	return fakeBBS.callsToConvergeLRPs
}

func (fakeBBS *FakeConvergerBBS) ConvergeLRPStartAuctions(kickPendingDuration time.Duration, expireClaimedDuration time.Duration) {
	fakeBBS.Lock()
	defer fakeBBS.Unlock()

	fakeBBS.callsToConvergeLRPStartAuctions++
}

func (fakeBBS *FakeConvergerBBS) CallsToConvergeLRPStartAuctions() int {
	fakeBBS.RLock()
	defer fakeBBS.RUnlock()

	return fakeBBS.callsToConvergeLRPStartAuctions
}

func (fakeBBS *FakeConvergerBBS) ConvergeLRPStopAuctions(kickPendingDuration time.Duration, expireClaimedDuration time.Duration) {
	fakeBBS.Lock()
	defer fakeBBS.Unlock()

	fakeBBS.callsToConvergeLRPStopAuctions++
}

func (fakeBBS *FakeConvergerBBS) CallsToConvergeLRPStopAuctions() int {
	fakeBBS.RLock()
	defer fakeBBS.RUnlock()

	return fakeBBS.callsToConvergeLRPStopAuctions
}

func (fakeBBS *FakeConvergerBBS) ConvergeTask(timeToClaim time.Duration, taskConvergenceInterval time.Duration) {
	fakeBBS.Lock()
	defer fakeBBS.Unlock()

	fakeBBS.convergeTimeToClaimTasks = timeToClaim
	fakeBBS.taskConvergenceInterval = taskConvergenceInterval
	fakeBBS.callsToConvergeTasks++
}

func (fakeBBS *FakeConvergerBBS) ConvergeTimeToClaimTasks() time.Duration {
	fakeBBS.RLock()
	defer fakeBBS.RUnlock()

	return fakeBBS.convergeTimeToClaimTasks
}

func (fakeBBS *FakeConvergerBBS) CallsToConvergeTasks() int {
	fakeBBS.RLock()
	defer fakeBBS.RUnlock()

	return fakeBBS.callsToConvergeTasks
}

func (fakeBBS *FakeConvergerBBS) NewConvergeLock(convergerID string, interval time.Duration) ifrit.Runner {
	panic("unimplemented")
	return new(fake_runner.FakeRunner)
}

func (fakeBBS *FakeConvergerBBS) SetMaintainConvergeLockError(err error) {
	fakeBBS.Lock()
	defer fakeBBS.Unlock()

	fakeBBS.maintainConvergeLockError = err
}

func (fakeBBS *FakeConvergerBBS) DesireLRP(lrp models.DesiredLRP) error {
	fakeBBS.Lock()
	defer fakeBBS.Unlock()

	fakeBBS.desiredLRPs = append(fakeBBS.desiredLRPs, lrp)
	return fakeBBS.DesireLRPErr
}

func (fakeBBS *FakeConvergerBBS) DesiredLRPs() []models.DesiredLRP {
	fakeBBS.RLock()
	defer fakeBBS.RUnlock()
	return fakeBBS.desiredLRPs
}

func (fakeBBS *FakeConvergerBBS) WatchForDesiredLRPChanges() (<-chan models.DesiredLRPChange, chan<- bool, <-chan error) {
	return fakeBBS.DesiredLRPChangeChan, fakeBBS.DesiredLRPStopChan, fakeBBS.DesiredLRPErrChan
}

func (fakeBBS *FakeConvergerBBS) RequestLRPStartAuction(lrp models.LRPStartAuction) error {
	fakeBBS.Lock()
	defer fakeBBS.Unlock()
	if fakeBBS.WhenRequestingLRPStartAuctions != nil {
		return fakeBBS.WhenRequestingLRPStartAuctions(lrp)
	}
	fakeBBS.lrpStartAuctions = append(fakeBBS.lrpStartAuctions, lrp)
	return fakeBBS.LRPStartAuctionErr
}

func (fakeBBS *FakeConvergerBBS) GetLRPStartAuctions() []models.LRPStartAuction {
	fakeBBS.RLock()
	defer fakeBBS.RUnlock()
	return fakeBBS.lrpStartAuctions
}

func (fakeBBS *FakeConvergerBBS) RequestLRPStopAuction(lrp models.LRPStopAuction) error {
	fakeBBS.Lock()
	defer fakeBBS.Unlock()
	if fakeBBS.WhenRequestingLRPStopAuctions != nil {
		return fakeBBS.WhenRequestingLRPStopAuctions(lrp)
	}
	fakeBBS.lrpStopAuctions = append(fakeBBS.lrpStopAuctions, lrp)
	return fakeBBS.LRPStopAuctionErr
}

func (fakeBBS *FakeConvergerBBS) GetLRPStopAuctions() []models.LRPStopAuction {
	fakeBBS.RLock()
	defer fakeBBS.RUnlock()
	return fakeBBS.lrpStopAuctions
}

func (fakeBBS *FakeConvergerBBS) RequestStopLRPInstance(lrp models.StopLRPInstance) error {
	fakeBBS.Lock()
	defer fakeBBS.Unlock()
	fakeBBS.stopLRPInstances = append(fakeBBS.stopLRPInstances, lrp)
	return fakeBBS.StopLRPInstanceErr
}

func (fakeBBS *FakeConvergerBBS) GetStopLRPInstances() []models.StopLRPInstance {
	fakeBBS.RLock()
	defer fakeBBS.RUnlock()
	return fakeBBS.stopLRPInstances
}

func (fakeBBS *FakeConvergerBBS) GetActualLRPsByProcessGuid(string) ([]models.ActualLRP, error) {
	fakeBBS.RLock()
	defer fakeBBS.RUnlock()
	return fakeBBS.ActualLRPs, fakeBBS.ActualLRPsErr
}

func (fakeBBS *FakeConvergerBBS) RemoveDesiredLRPByProcessGuid(processGuid string) error {
	fakeBBS.Lock()
	defer fakeBBS.Unlock()
	fakeBBS.removeDesiredLRPProcessGuids = append(fakeBBS.removeDesiredLRPProcessGuids, processGuid)
	return fakeBBS.removeDesiredLRPProcessGuidsErr
}

func (fakeBBS *FakeConvergerBBS) GetRemovedDesiredLRPProcessGuids() []string {
	fakeBBS.RLock()
	defer fakeBBS.RUnlock()
	return fakeBBS.removeDesiredLRPProcessGuids
}
