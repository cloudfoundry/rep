package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cloudfoundry-incubator/auction/communication/http/auction_http_handlers"
	"github.com/cloudfoundry-incubator/auction/communication/http/routes"
	"github.com/cloudfoundry-incubator/cf-debug-server"
	"github.com/cloudfoundry-incubator/cf-lager"
	"github.com/cloudfoundry-incubator/executor"
	executorclient "github.com/cloudfoundry-incubator/executor/http/client"
	"github.com/cloudfoundry-incubator/rep/auction_cell_rep"
	"github.com/cloudfoundry-incubator/rep/gatherer"
	"github.com/cloudfoundry-incubator/rep/harvester"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
	"github.com/cloudfoundry-incubator/rep/maintain"
	"github.com/cloudfoundry-incubator/rep/reaper"
	"github.com/cloudfoundry-incubator/rep/stop_lrp_listener"
	"github.com/cloudfoundry-incubator/rep/task_scheduler"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/dropsonde"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/gunk/workpool"
	"github.com/cloudfoundry/loggregatorlib/cfcomponent/localip"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/timer"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/http_server"
	"github.com/tedsuo/ifrit/sigmon"
	"github.com/tedsuo/rata"
)

var etcdCluster = flag.String(
	"etcdCluster",
	"http://127.0.0.1:4001",
	"comma-separated list of etcd addresses (http://ip:port)",
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

var auctionListenAddr = flag.String(
	"auctionListenAddr",
	"0.0.0.0:1800",
	"host:port to serve auction requests on",
)

var lrpHost = flag.String(
	"lrpHost",
	"",
	"address to route traffic to for LRP access",
)

var stack = flag.String(
	"stack",
	"",
	"the rep stack - must be specified",
)

var cellID = flag.String(
	"cellID",
	"",
	"the ID used by the rep to identify itself to external systems - must be specified",
)

var pollingInterval = flag.Duration(
	"pollingInterval",
	30*time.Second,
	"the interval on which to scan the executor",
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

	if *cellID == "" {
		log.Fatalf("-cellID must be specified")
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
	removeActualLrpFromBBS(bbs, *cellID, logger)

	executorClient := executorclient.New(http.DefaultClient, *executorURL)
	lrpStopper := initializeLRPStopper(*cellID, bbs, executorClient, logger)

	taskCompleter := reaper.NewTaskCompleter(bbs, logger)
	taskContainerReaper := reaper.NewTaskContainerReaper(executorClient, logger)
	actualLRPReaper := reaper.NewActualLRPReaper(bbs, logger)

	bulkProcessor, eventConsumer := initializeHarvesters(logger, *pollingInterval, executorClient, bbs)

	gatherer := gatherer.NewGatherer(*pollingInterval, timer.NewTimer(), []gatherer.Processor{
		bulkProcessor,
		taskCompleter,
		taskContainerReaper,
		actualLRPReaper,
	}, *cellID, bbs, executorClient, logger)

	auctionServer, address := initializeAuctionServer(lrpStopper, bbs, executorClient, logger)

	group := grouper.NewOrdered(os.Interrupt, grouper.Members{
		{"auction-server", auctionServer},
		{"heartbeater", initializeCellHeartbeat(address, bbs, executorClient, logger)},
		{"task-rep", initializeTaskRep(*cellID, bbs, logger, executorClient)},
		{"stop-lrp-listener", initializeStopLRPListener(lrpStopper, bbs, logger)},
		{"gatherer", gatherer},
		{"event-consumer", eventConsumer},
	})

	monitor := ifrit.Invoke(sigmon.New(group))
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
) (gatherer.Processor, ifrit.Runner) {
	taskProcessor := harvester.NewTaskProcessor(
		logger,
		bbs,
		executorClient,
	)

	lrpProcessor := harvester.NewLRPProcessor(
		*cellID,
		*lrpHost,
		logger,
		bbs,
		executorClient,
	)

	containerProcessor := harvester.NewContainerProcessor(
		logger,
		taskProcessor,
		lrpProcessor,
	)

	bulkProcessor := harvester.NewBulkContainerProcessor(
		containerProcessor,
		logger,
	)

	eventConsumer := harvester.NewEventConsumer(
		logger,
		executorClient,
		containerProcessor,
	)

	return bulkProcessor, eventConsumer
}

func removeActualLrpFromBBS(bbs Bbs.RepBBS, cellID string, logger lager.Logger) {
	for {
		lrps, err := bbs.ActualLRPsByCellID(cellID)
		if err != nil {
			logger.Error("failed-to-get-actual-lrps-by-cell-id", err, lager.Data{"cell-id": cellID})
			time.Sleep(time.Second)
			continue
		}

		for _, lrp := range lrps {
			err = bbs.RemoveActualLRP(lrp)
			if err != nil {
				logger.Error("failed-to-remove-actual-lrps", err, lager.Data{"cell-id": cellID, "actual-lrp": lrp, "total-lrps": len(lrps)})
				time.Sleep(time.Second)
				continue
			}
		}

		break
	}
}

func initializeCellHeartbeat(address string, bbs Bbs.RepBBS, executorClient executor.Client, logger lager.Logger) ifrit.Runner {
	cellPresence := models.CellPresence{
		CellID:     *cellID,
		RepAddress: address,
		Stack:      *stack,
	}

	heartbeat := bbs.NewCellHeartbeat(cellPresence, *heartbeatInterval)
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

func initializeTaskRep(cellID string, bbs Bbs.RepBBS, logger lager.Logger, executorClient executor.Client) *task_scheduler.TaskScheduler {
	return task_scheduler.New(cellID, bbs, logger, *stack, executorClient)
}

func initializeLRPStopper(guid string, bbs Bbs.RepBBS, executorClient executor.Client, logger lager.Logger) lrp_stopper.LRPStopper {
	return lrp_stopper.New(guid, bbs, executorClient, logger)
}

func initializeStopLRPListener(stopper lrp_stopper.LRPStopper, bbs Bbs.RepBBS, logger lager.Logger) ifrit.Runner {
	return stop_lrp_listener.New(stopper, bbs, logger)
}

func initializeAuctionServer(
	stopper lrp_stopper.LRPStopper,
	bbs Bbs.RepBBS,
	executorClient executor.Client,
	logger lager.Logger,
) (ifrit.Runner, string) {
	auctionCellRep := auction_cell_rep.New(*cellID, *stack, stopper, bbs, executorClient, logger)
	handlers := auction_http_handlers.New(auctionCellRep, logger)
	router, err := rata.NewRouter(routes.Routes, handlers)
	if err != nil {
		logger.Fatal("failed-to-construct-auction-router", err)
	}

	ip, err := localip.LocalIP()
	if err != nil {
		logger.Fatal("failed-to-fetch-ip", err)
	}

	port := strings.Split(*auctionListenAddr, ":")[1]
	address := fmt.Sprintf("http://%s:%s", ip, port)

	return http_server.New(*auctionListenAddr, router), address
}
