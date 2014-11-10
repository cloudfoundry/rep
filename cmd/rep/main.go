package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cloudfoundry-incubator/auction/auctionrep"
	"github.com/cloudfoundry-incubator/auction/communication/nats/auction_nats_server"
	"github.com/cloudfoundry-incubator/cf-debug-server"
	"github.com/cloudfoundry-incubator/cf-lager"
	"github.com/cloudfoundry-incubator/executor"
	executorclient "github.com/cloudfoundry-incubator/executor/http/client"
	"github.com/cloudfoundry-incubator/rep/api"
	"github.com/cloudfoundry-incubator/rep/api/lrprunning"
	"github.com/cloudfoundry-incubator/rep/auction_delegate"
	"github.com/cloudfoundry-incubator/rep/harvester"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
	"github.com/cloudfoundry-incubator/rep/maintain"
	"github.com/cloudfoundry-incubator/rep/reaper"
	"github.com/cloudfoundry-incubator/rep/stop_lrp_listener"
	"github.com/cloudfoundry-incubator/rep/task_scheduler"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/dropsonde"
	"github.com/cloudfoundry/gunk/diegonats"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/gunk/workpool"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/timer"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/http_server"
	"github.com/tedsuo/ifrit/sigmon"
)

var etcdCluster = flag.String(
	"etcdCluster",
	"http://127.0.0.1:4001",
	"comma-separated list of etcd addresses (http://ip:port)",
)

var natsAddresses = flag.String(
	"natsAddresses",
	"127.0.0.1:4222",
	"comma-separated list of NATS addresses (ip:port)",
)

var natsUsername = flag.String(
	"natsUsername",
	"nats",
	"Username to connect to nats",
)

var natsPassword = flag.String(
	"natsPassword",
	"nats",
	"Password for nats user",
)

var heartbeatInterval = flag.Duration(
	"heartbeatInterval",
	60*time.Second,
	"the interval between heartbeats for maintaining presence",
)

var executorURL = flag.String(
	"executorURL",
	"http://127.0.0.1:1700",
	"location of executor to represent",
)

var lrpHost = flag.String(
	"lrpHost",
	"",
	"address to route traffic to for LRP access",
)

var listenAddr = flag.String(
	"listenAddr",
	"0.0.0.0:20515",
	"host:port to listen on for job completion",
)

var stack = flag.String(
	"stack",
	"",
	"the rep stack - must be specified",
)

var executorID = flag.String(
	"executorID",
	"",
	"the ID used by the rep to identify itself to external systems - must be specified",
)

var taskCompletePollingInterval = flag.Duration(
	"taskCompletePollingInterval",
	30*time.Second,
	"the interval on which to look for completed tasks",
)

var actualLRPReapingInterval = flag.Duration(
	"actualLRPReapingInterval",
	30*time.Second,
	"the interval on which to reap actual LRPs in BBS that have no corresponding container",
)

var taskReapingInterval = flag.Duration(
	"taskReapingInterval",
	30*time.Second,
	"the interval on which to mark tasks as failed in BBS that have no corresponding container",
)

var dropsondeOrigin = flag.String(
	"dropsondeOrigin",
	"rep",
	"Origin identifier for dropsonde-emitted metrics.",
)

var dropsondeDestination = flag.String(
	"dropsondeDestination",
	"localhost:3457",
	"Destination for dropsonde-emitted metrics.",
)

func main() {
	flag.Parse()

	if *executorID == "" {
		log.Fatalf("-executorID must be specified")
	}

	if *stack == "" {
		log.Fatalf("-stack must be specified")
	}

	if *lrpHost == "" {
		log.Fatalf("-lrpHost must be specified")
	}

	cf_debug_server.Run()

	logger := cf_lager.New("rep")
	initializeDropsonde(logger)
	bbs := initializeRepBBS(logger)
	removeActualLrpFromBBS(bbs, *executorID, logger)

	natsClient := diegonats.NewClient()
	natsClientRunner := diegonats.NewClientRunner(*natsAddresses, *natsUsername, *natsPassword, logger, natsClient)

	executorClient := executorclient.New(http.DefaultClient, *executorURL)
	lrpStopper := initializeLRPStopper(*executorID, bbs, executorClient, logger)

	executorPoller, eventConsumer := initializeHarvesters(logger, *taskCompletePollingInterval, executorClient, bbs)

	actualLRPReaper := reaper.NewActualLRPReaper(*actualLRPReapingInterval, timer.NewTimer(), *executorID, bbs, executorClient, logger)

	taskReaper := reaper.NewTaskReaper(*taskReapingInterval, timer.NewTimer(), *executorID, bbs, executorClient, logger)

	group := grouper.NewOrdered(os.Interrupt, grouper.Members{
		{"nats-client", natsClientRunner},
		{"heartbeater", initializeExecutorHeartbeat(bbs, executorClient, logger)},
		{"task-rep", initializeTaskRep(*executorID, bbs, logger, executorClient)},
		{"stop-lrp-listener", initializeStopLRPListener(lrpStopper, bbs, logger)},
		{"api-server", initializeAPIServer(*executorID, bbs, logger, executorClient)},
		{"auction-server", ifrit.RunFunc(func(signals <-chan os.Signal, ready chan<- struct{}) error {
			return initializeAuctionNatsServer(*executorID, lrpStopper, bbs, executorClient, natsClient, logger).Run(signals, ready)
		})},
		{"executor-poller", executorPoller},
		{"actual-lrp-reaper", actualLRPReaper},
		{"task-reaper", taskReaper},
		{"event-consumer", eventConsumer},
	})

	monitor := ifrit.Envoke(sigmon.New(group))
	logger.Info("started")

	<-monitor.Wait()
	logger.Info("shutting-down")
}

func initializeDropsonde(logger lager.Logger) {
	err := dropsonde.Initialize(*dropsondeDestination, *dropsondeOrigin)
	if err != nil {
		logger.Error("failed to initialize dropsonde: %v", err)
	}
}

func initializeHarvesters(
	logger lager.Logger,
	pollInterval time.Duration,
	executorClient executor.Client,
	bbs Bbs.RepBBS,
) (ifrit.Runner, ifrit.Runner) {
	processor := harvester.NewProcessor(
		logger,
		bbs,
		executorClient,
	)

	executorPoller := harvester.NewPoller(
		pollInterval,
		timer.NewTimer(),
		executorClient,
		processor,
		bbs,
		*executorID,
		logger,
	)

	eventConsumer := harvester.NewEventConsumer(
		executorClient,
		processor,
	)

	return executorPoller, eventConsumer
}

func removeActualLrpFromBBS(bbs Bbs.RepBBS, executorID string, logger lager.Logger) {
	for {
		lrps, err := bbs.GetAllActualLRPsByExecutorID(executorID)
		if err != nil {
			logger.Error("failed-to-get-actual-lrps-by-executor-id", err, lager.Data{"executor-id": executorID})
			time.Sleep(time.Second)
			continue
		}

		for _, lrp := range lrps {
			err = bbs.RemoveActualLRP(lrp)
			if err != nil {
				logger.Error("failed-to-remove-actual-lrps", err, lager.Data{"executor-id": executorID, "actual-lrp": lrp, "total-lrps": len(lrps)})
				time.Sleep(time.Second)
				continue
			}
		}

		break
	}
}

func initializeExecutorHeartbeat(bbs Bbs.RepBBS, executorClient executor.Client, logger lager.Logger) ifrit.Runner {
	executorPresence := models.ExecutorPresence{
		ExecutorID: *executorID,
		Stack:      *stack,
	}

	heartbeat := bbs.NewExecutorHeartbeat(executorPresence, *heartbeatInterval)
	return maintain.New(executorClient, heartbeat, logger, *heartbeatInterval, timer.NewTimer())
}

func initializeRepBBS(logger lager.Logger) Bbs.RepBBS {
	etcdAdapter := etcdstoreadapter.NewETCDStoreAdapter(
		strings.Split(*etcdCluster, ","),
		workpool.NewWorkPool(10),
	)

	err := etcdAdapter.Connect()
	if err != nil {
		logger.Fatal("failed-to-connect-to-etcd", err)
	}

	return Bbs.NewRepBBS(etcdAdapter, timeprovider.NewTimeProvider(), logger)
}

func initializeTaskRep(executorID string, bbs Bbs.RepBBS, logger lager.Logger, executorClient executor.Client) *task_scheduler.TaskScheduler {
	return task_scheduler.New(executorID, bbs, logger, *stack, executorClient)
}

func initializeLRPStopper(guid string, bbs Bbs.RepBBS, executorClient executor.Client, logger lager.Logger) lrp_stopper.LRPStopper {
	return lrp_stopper.New(guid, bbs, executorClient, logger)
}

func initializeStopLRPListener(stopper lrp_stopper.LRPStopper, bbs Bbs.RepBBS, logger lager.Logger) ifrit.Runner {
	return stop_lrp_listener.New(stopper, bbs, logger)
}

func initializeAPIServer(executorID string, bbs Bbs.RepBBS, logger lager.Logger, executorClient executor.Client) ifrit.Runner {
	lrpRunningHandler := lrprunning.NewHandler(executorID, bbs, executorClient, *lrpHost, logger)

	apiHandler, err := api.NewServer(lrpRunningHandler)
	if err != nil {
		panic("failed to initialize api server: " + err.Error())
	}

	return http_server.New(*listenAddr, apiHandler)
}

func initializeAuctionNatsServer(
	executorID string,
	stopper lrp_stopper.LRPStopper,
	bbs Bbs.RepBBS,
	executorClient executor.Client,
	natsClient diegonats.NATSClient,
	logger lager.Logger,
) *auction_nats_server.AuctionNATSServer {
	auctionDelegate := auction_delegate.New(executorID, stopper, bbs, executorClient, logger)
	auctionRep := auctionrep.New(executorID, auctionDelegate)
	return auction_nats_server.New(natsClient, auctionRep, logger)
}
