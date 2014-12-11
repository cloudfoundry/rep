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
	auctionroutes "github.com/cloudfoundry-incubator/auction/communication/http/routes"
	"github.com/cloudfoundry-incubator/cf-debug-server"
	"github.com/cloudfoundry-incubator/cf-lager"
	"github.com/cloudfoundry-incubator/executor"
	executorclient "github.com/cloudfoundry-incubator/executor/http/client"
	"github.com/cloudfoundry-incubator/rep/auction_cell_rep"
	"github.com/cloudfoundry-incubator/rep/gatherer"
	"github.com/cloudfoundry-incubator/rep/harvester"
	repserver "github.com/cloudfoundry-incubator/rep/http_server"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
	"github.com/cloudfoundry-incubator/rep/maintain"
	"github.com/cloudfoundry-incubator/rep/reaper"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	bbsroutes "github.com/cloudfoundry-incubator/runtime-schema/routes"
	"github.com/cloudfoundry/dropsonde"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/gunk/workpool"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/localip"
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

var listenAddr = flag.String(
	"listenAddr",
	"0.0.0.0:1800",
	"host:port to serve auction and LRP stop requests on",
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

	executorClient := executorclient.New(http.DefaultClient, *executorURL)
	lrpStopper := initializeLRPStopper(*cellID, bbs, executorClient, logger)

	taskCompleter := reaper.NewTaskCompleter(bbs, logger)
	taskContainerReaper := reaper.NewTaskContainerReaper(executorClient, logger)
	actualLRPReaper := reaper.NewActualLRPReaper(bbs, logger)

	bulkProcessor, eventConsumer := initializeHarvesters(logger, *pollingInterval, executorClient, bbs)

	gatherer := gatherer.NewGatherer(*pollingInterval, timeprovider.NewTimeProvider(), []gatherer.Processor{
		bulkProcessor,
		taskCompleter,
		taskContainerReaper,
		actualLRPReaper,
	}, *cellID, bbs, executorClient, logger)

	server, address := initializeServer(lrpStopper, bbs, executorClient, logger)

	group := grouper.NewOrdered(os.Interrupt, grouper.Members{
		{"server", server},
		{"heartbeater", initializeCellHeartbeat(address, bbs, executorClient, logger)},
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

func initializeCellHeartbeat(address string, bbs Bbs.RepBBS, executorClient executor.Client, logger lager.Logger) ifrit.Runner {
	cellPresence := models.CellPresence{
		CellID:     *cellID,
		RepAddress: address,
		Stack:      *stack,
	}

	heartbeat := bbs.NewCellHeartbeat(cellPresence, *heartbeatInterval)
	return maintain.New(executorClient, heartbeat, logger, *heartbeatInterval, timeprovider.NewTimeProvider())
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

func initializeLRPStopper(guid string, bbs Bbs.RepBBS, executorClient executor.Client, logger lager.Logger) lrp_stopper.LRPStopper {
	return lrp_stopper.New(guid, bbs, executorClient, logger)
}

func initializeServer(
	stopper lrp_stopper.LRPStopper,
	bbs Bbs.RepBBS,
	executorClient executor.Client,
	logger lager.Logger,
) (ifrit.Runner, string) {
	auctionCellRep := auction_cell_rep.New(*cellID, *stack, bbs, executorClient, logger)
	handlers := auction_http_handlers.New(auctionCellRep, logger)

	handlers[bbsroutes.StopLRPInstance] = repserver.NewStopLRPInstanceHandler(logger, stopper)
	routes := append(auctionroutes.Routes, bbsroutes.StopLRPRoutes...)

	router, err := rata.NewRouter(routes, handlers)
	if err != nil {
		logger.Fatal("failed-to-construct-router", err)
	}

	ip, err := localip.LocalIP()
	if err != nil {
		logger.Fatal("failed-to-fetch-ip", err)
	}

	port := strings.Split(*listenAddr, ":")[1]
	address := fmt.Sprintf("http://%s:%s", ip, port)

	return http_server.New(*listenAddr, router), address
}
