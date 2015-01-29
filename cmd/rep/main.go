package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/cloudfoundry-incubator/auction/communication/http/auction_http_handlers"
	auctionroutes "github.com/cloudfoundry-incubator/auction/communication/http/routes"
	"github.com/cloudfoundry-incubator/cf-debug-server"
	"github.com/cloudfoundry-incubator/cf-lager"
	"github.com/cloudfoundry-incubator/cf_http"
	"github.com/cloudfoundry-incubator/executor"
	executorclient "github.com/cloudfoundry-incubator/executor/http/client"
	"github.com/cloudfoundry-incubator/rep/auction_cell_rep"
	"github.com/cloudfoundry-incubator/rep/evacuation"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context"
	"github.com/cloudfoundry-incubator/rep/generator"
	"github.com/cloudfoundry-incubator/rep/harmonizer"
	repserver "github.com/cloudfoundry-incubator/rep/http_server"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
	"github.com/cloudfoundry-incubator/rep/maintain"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/services_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	bbsroutes "github.com/cloudfoundry-incubator/runtime-schema/routes"
	"github.com/cloudfoundry/dropsonde"
	"github.com/cloudfoundry/gunk/workpool"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"
	"github.com/nu7hatch/gouuid"
	"github.com/pivotal-golang/clock"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/localip"
	"github.com/pivotal-golang/operationq"
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
	services_bbs.CELL_HEARTBEAT_INTERVAL,
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

var zone = flag.String(
	"zone",
	"",
	"the availability zone associated with the rep",
)

var pollingInterval = flag.Duration(
	"pollingInterval",
	30*time.Second,
	"the interval on which to scan the executor",
)

var communicationTimeout = flag.Duration(
	"communicationTimeout",
	10*time.Second,
	"Timeout applied to all HTTP requests.",
)

var evacuationTimeout = flag.Duration(
	"evacuationTimeout",
	3*time.Minute,
	"Timeout to wait for evacuation to complete",
)

var evacuationPollingInterval = flag.Duration(
	"evacuationPollingInterval",
	30*time.Second,
	"the interval on which to scan the executor for containers during evacuation",
)

const (
	dropsondeDestination = "localhost:3457"
	dropsondeOrigin      = "rep"
)

func main() {
	cf_debug_server.AddFlags(flag.CommandLine)
	cf_lager.AddFlags(flag.CommandLine)
	flag.Parse()

	cf_http.Initialize(*communicationTimeout)

	logger := cf_lager.New("rep")
	initializeDropsonde(logger)

	if *cellID == "" {
		log.Fatalf("-cellID must be specified")
	}

	if *stack == "" {
		log.Fatalf("-stack must be specified")
	}

	bbs := initializeRepBBS(logger)

	clock := clock.NewClock()

	executorClient := executorclient.New(cf_http.NewClient(), cf_http.NewStreamingClient(), *executorURL)

	evacuatable, evacuationReporter := evacuation_context.New()
	evacuator := evacuation.NewEvacuator(logger, executorClient, bbs, evacuatable, *cellID, *evacuationTimeout, *evacuationPollingInterval, clock)

	httpServer, address := initializeServer(bbs, executorClient, evacuationReporter, logger)
	opGenerator := generator.New(*cellID, bbs, executorClient, evacuationReporter)

	// only one outstanding operation per container is necessary
	queue := operationq.NewSlidingQueue(1)

	members := grouper.Members{
		{"http_server", httpServer},
		{"heartbeater", initializeCellHeartbeat(address, bbs, executorClient, logger)},
		{"bulker", harmonizer.NewBulker(logger, *pollingInterval, clock, opGenerator, queue)},
		{"event-consumer", harmonizer.NewEventConsumer(logger, opGenerator, queue)},
		{"evacuator", evacuator},
	}

	if dbgAddr := cf_debug_server.DebugAddress(flag.CommandLine); dbgAddr != "" {
		members = append(grouper.Members{
			{"debug-server", cf_debug_server.Runner(dbgAddr)},
		}, members...)
	}

	group := grouper.NewOrdered(os.Interrupt, members)

	monitor := ifrit.Invoke(sigmon.New(group, syscall.SIGUSR1))

	logger.Info("started", lager.Data{"cell-id": *cellID})

	err := <-monitor.Wait()
	if err != nil {
		logger.Error("exited-with-failure", err)
		os.Exit(1)
	}

	logger.Info("exited")
}

func initializeDropsonde(logger lager.Logger) {
	err := dropsonde.Initialize(dropsondeDestination, dropsondeOrigin)
	if err != nil {
		logger.Error("failed to initialize dropsonde: %v", err)
	}
}

func initializeCellHeartbeat(address string, bbs Bbs.RepBBS, executorClient executor.Client, logger lager.Logger) ifrit.Runner {
	cellPresence := models.CellPresence{
		CellID:     *cellID,
		RepAddress: address,
		Stack:      *stack,
		Zone:       *zone,
	}

	heartbeat := bbs.NewCellHeartbeat(cellPresence, *heartbeatInterval)
	return maintain.New(executorClient, heartbeat, logger, *heartbeatInterval, clock.NewClock())
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

	return Bbs.NewRepBBS(etcdAdapter, clock.NewClock(), logger)
}

func initializeLRPStopper(guid string, executorClient executor.Client, logger lager.Logger) lrp_stopper.LRPStopper {
	return lrp_stopper.New(guid, executorClient, logger)
}

func initializeServer(
	bbs Bbs.RepBBS,
	executorClient executor.Client,
	evacuationReporter evacuation_context.EvacuationReporter,
	logger lager.Logger,
) (ifrit.Runner, string) {
	lrpStopper := initializeLRPStopper(*cellID, executorClient, logger)

	auctionCellRep := auction_cell_rep.New(*cellID, *stack, *zone, generateGuid, bbs, executorClient, evacuationReporter, logger)
	handlers := auction_http_handlers.New(auctionCellRep, logger)

	handlers[bbsroutes.StopLRPInstance] = repserver.NewStopLRPInstanceHandler(logger, lrpStopper)
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

func generateGuid() (string, error) {
	guid, err := uuid.NewV4()
	if err != nil {
		return "", err
	}
	return guid.String(), nil
}
