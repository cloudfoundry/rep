package bbs

import (
	"time"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/lock_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/lrp_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/services_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/start_auction_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/stop_auction_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/task_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/storeadapter"
	"github.com/pivotal-golang/lager"
)

//Bulletin Board System/Store

type RepBBS interface {
	//services
	MaintainExecutorPresence(heartbeatInterval time.Duration, executorPresence models.ExecutorPresence) (services_bbs.Presence, <-chan bool, error)

	//task
	WatchForDesiredTask() (<-chan models.Task, chan<- bool, <-chan error)
	ClaimTask(taskGuid string, executorID string) error
	StartTask(taskGuid string, executorID string, containerHandle string) error
	CompleteTask(taskGuid string, failed bool, failureReason string, result string) error

	///lrp
	ReportActualLRPAsStarting(lrp models.ActualLRP, executorID string) error
	ReportActualLRPAsRunning(lrp models.ActualLRP, executorId string) error
	RemoveActualLRP(lrp models.ActualLRP) error
	RemoveActualLRPForIndex(processGuid string, index int, instanceGuid string) error
	WatchForStopLRPInstance() (<-chan models.StopLRPInstance, chan<- bool, <-chan error)
	ResolveStopLRPInstance(stopInstance models.StopLRPInstance) error
}

type ConvergerBBS interface {
	//lrp
	ConvergeLRPs()
	GetActualLRPsByProcessGuid(string) ([]models.ActualLRP, error)
	RequestStopLRPInstance(stopInstance models.StopLRPInstance) error
	WatchForDesiredLRPChanges() (<-chan models.DesiredLRPChange, chan<- bool, <-chan error)

	//start auction
	ConvergeLRPStartAuctions(kickPendingDuration time.Duration, expireClaimedDuration time.Duration)
	RequestLRPStartAuction(models.LRPStartAuction) error

	//stop auction
	ConvergeLRPStopAuctions(kickPendingDuration time.Duration, expireClaimedDuration time.Duration)
	RequestLRPStopAuction(models.LRPStopAuction) error

	//task
	ConvergeTask(timeToClaim time.Duration, converganceInterval time.Duration)

	//lock
	MaintainConvergeLock(interval time.Duration, executorID string) (disappeared <-chan bool, stop chan<- chan bool, err error)

	//services
	GetAvailableFileServer() (string, error)
}

type TPSBBS interface {
	//lrp
	GetActualLRPsByProcessGuid(string) ([]models.ActualLRP, error)
}

type NsyncBBS interface {
	// lrp
	DesireLRP(models.DesiredLRP) error
	RemoveDesiredLRPByProcessGuid(guid string) error
	GetAllDesiredLRPsByDomain(domain string) ([]models.DesiredLRP, error)
	ChangeDesiredLRP(change models.DesiredLRPChange) error
}

type AuctioneerBBS interface {
	//services
	GetAllExecutors() ([]models.ExecutorPresence, error)

	//start auction
	WatchForLRPStartAuction() (<-chan models.LRPStartAuction, chan<- bool, <-chan error)
	ClaimLRPStartAuction(models.LRPStartAuction) error
	ResolveLRPStartAuction(models.LRPStartAuction) error

	//stop auction
	WatchForLRPStopAuction() (<-chan models.LRPStopAuction, chan<- bool, <-chan error)
	ClaimLRPStopAuction(models.LRPStopAuction) error
	ResolveLRPStopAuction(models.LRPStopAuction) error

	//lock
	MaintainAuctioneerLock(interval time.Duration, auctioneerID string) (<-chan bool, chan<- chan bool, error)
}

type StagerBBS interface {
	//task
	WatchForCompletedTask() (<-chan models.Task, chan<- bool, <-chan error)
	DesireTask(models.Task) error
	ResolvingTask(taskGuid string) error
	ResolveTask(taskGuid string) error

	//services
	GetAvailableFileServer() (string, error)
}

type MetricsBBS interface {
	//task
	GetAllTasks() ([]models.Task, error)

	//services
	GetServiceRegistrations() (models.ServiceRegistrations, error)
}

type FileServerBBS interface {
	//services
	MaintainFileServerPresence(
		heartbeatInterval time.Duration,
		fileServerURL string,
		fileServerId string,
	) (presence services_bbs.Presence, disappeared <-chan bool, err error)
}

type RouteEmitterBBS interface {
	// lrp
	WatchForDesiredLRPChanges() (<-chan models.DesiredLRPChange, chan<- bool, <-chan error)
	WatchForActualLRPChanges() (<-chan models.ActualLRPChange, chan<- bool, <-chan error)
	GetAllDesiredLRPs() ([]models.DesiredLRP, error)
	GetRunningActualLRPs() ([]models.ActualLRP, error)
	GetDesiredLRPByProcessGuid(processGuid string) (models.DesiredLRP, error)
	GetRunningActualLRPsByProcessGuid(processGuid string) ([]models.ActualLRP, error)
}

type VeritasBBS interface {
	//task
	GetAllTasks() ([]models.Task, error)

	//lrp
	GetAllDesiredLRPs() ([]models.DesiredLRP, error)
	GetAllActualLRPs() ([]models.ActualLRP, error)
	GetAllStopLRPInstances() ([]models.StopLRPInstance, error)

	//start auctions
	GetAllLRPStartAuctions() ([]models.LRPStartAuction, error)

	//stop auctions
	GetAllLRPStopAuctions() ([]models.LRPStopAuction, error)

	//services
	GetAllExecutors() ([]models.ExecutorPresence, error)
	GetAllFileServers() ([]string, error)
}

func NewRepBBS(store storeadapter.StoreAdapter, timeProvider timeprovider.TimeProvider, logger lager.Logger) RepBBS {
	return NewBBS(store, timeProvider, logger)
}

func NewConvergerBBS(store storeadapter.StoreAdapter, timeProvider timeprovider.TimeProvider, logger lager.Logger) ConvergerBBS {
	return NewBBS(store, timeProvider, logger)
}

func NewNsyncBBS(store storeadapter.StoreAdapter, timeProvider timeprovider.TimeProvider, logger lager.Logger) NsyncBBS {
	return NewBBS(store, timeProvider, logger)
}

func NewAuctioneerBBS(store storeadapter.StoreAdapter, timeProvider timeprovider.TimeProvider, logger lager.Logger) AuctioneerBBS {
	return NewBBS(store, timeProvider, logger)
}

func NewStagerBBS(store storeadapter.StoreAdapter, timeProvider timeprovider.TimeProvider, logger lager.Logger) StagerBBS {
	return NewBBS(store, timeProvider, logger)
}

func NewMetricsBBS(store storeadapter.StoreAdapter, timeProvider timeprovider.TimeProvider, logger lager.Logger) MetricsBBS {
	return NewBBS(store, timeProvider, logger)
}

func NewFileServerBBS(store storeadapter.StoreAdapter, timeProvider timeprovider.TimeProvider, logger lager.Logger) FileServerBBS {
	return NewBBS(store, timeProvider, logger)
}

func NewRouteEmitterBBS(store storeadapter.StoreAdapter, timeProvider timeprovider.TimeProvider, logger lager.Logger) RouteEmitterBBS {
	return NewBBS(store, timeProvider, logger)
}

func NewTPSBBS(store storeadapter.StoreAdapter, timeProvider timeprovider.TimeProvider, logger lager.Logger) TPSBBS {
	return NewBBS(store, timeProvider, logger)
}

func NewVeritasBBS(store storeadapter.StoreAdapter, timeProvider timeprovider.TimeProvider, logger lager.Logger) VeritasBBS {
	return NewBBS(store, timeProvider, logger)
}

func NewBBS(store storeadapter.StoreAdapter, timeProvider timeprovider.TimeProvider, logger lager.Logger) *BBS {
	return &BBS{
		LockBBS:         lock_bbs.New(store),
		LRPBBS:          lrp_bbs.New(store, timeProvider, logger.Session("lrp-bbs")),
		StartAuctionBBS: start_auction_bbs.New(store, timeProvider, logger.Session("lrp-start-auction-bbs")),
		StopAuctionBBS:  stop_auction_bbs.New(store, timeProvider, logger.Session("lrp-stop-auction-bbs")),
		ServicesBBS:     services_bbs.New(store, logger.Session("services-bbs")),
		TaskBBS:         task_bbs.New(store, timeProvider, logger.Session("task-bbs")),
	}
}

type BBS struct {
	*lock_bbs.LockBBS
	*lrp_bbs.LRPBBS
	*start_auction_bbs.StartAuctionBBS
	*stop_auction_bbs.StopAuctionBBS
	*services_bbs.ServicesBBS
	*task_bbs.TaskBBS
}
