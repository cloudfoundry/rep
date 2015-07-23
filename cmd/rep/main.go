package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/cloudfoundry-incubator/auction/communication/http/auction_http_handlers"
	auctionroutes "github.com/cloudfoundry-incubator/auction/communication/http/routes"
	"github.com/cloudfoundry-incubator/bbs"
	"github.com/cloudfoundry-incubator/cf-debug-server"
	cf_lager "github.com/cloudfoundry-incubator/cf-lager"
	"github.com/cloudfoundry-incubator/cf_http"
	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/cloudfoundry-incubator/executor"
	executorinit "github.com/cloudfoundry-incubator/executor/initializer"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/auction_cell_rep"
	"github.com/cloudfoundry-incubator/rep/evacuation"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context"
	"github.com/cloudfoundry-incubator/rep/generator"
	"github.com/cloudfoundry-incubator/rep/generator/internal"
	"github.com/cloudfoundry-incubator/rep/harmonizer"
	repserver "github.com/cloudfoundry-incubator/rep/http_server"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
	"github.com/cloudfoundry-incubator/rep/maintain"
	legacybbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/lock_bbs"
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

var sessionName = flag.String(
	"sessionName",
	"rep",
	"consul session name",
)

var consulCluster = flag.String(
	"consulCluster",
	"",
	"comma-separated list of consul server URLs (scheme://ip:port)",
)

var lockTTL = flag.Duration(
	"lockTTL",
	lock_bbs.LockTTL,
	"TTL for service lock",
)

var lockRetryInterval = flag.Duration(
	"lockRetryInterval",
	lock_bbs.RetryInterval,
	"interval to wait before retrying a failed lock acquisition",
)

var receptorTaskHandlerURL = flag.String(
	"receptorTaskHandlerURL",
	"http://127.0.0.1:1169",
	"location of receptor task handler",
)

var listenAddr = flag.String(
	"listenAddr",
	"0.0.0.0:1800",
	"host:port to serve auction and LRP stop requests on",
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
	10*time.Minute,
	"Timeout to wait for evacuation to complete",
)

var evacuationPollingInterval = flag.Duration(
	"evacuationPollingInterval",
	10*time.Second,
	"the interval on which to scan the executor during evacuation",
)

var bbsAddress = flag.String(
	"bbsAddress",
	"",
	"Address to the BBS Server",
)

type stackPathMap rep.StackPathMap

func (s *stackPathMap) String() string {
	return fmt.Sprintf("%v", *s)
}

func (s *stackPathMap) Set(value string) error {
	parts := strings.SplitN(value, ":", 2)
	if len(parts) != 2 {
		return errors.New("Invalid preloaded RootFS value: not of the form 'stack-name:path'")
	}

	if parts[0] == "" {
		return errors.New("Invalid preloaded RootFS value: blank stack")
	}

	if parts[1] == "" {
		return errors.New("Invalid preloaded RootFS value: blank path")
	}

	(*s)[parts[0]] = parts[1]
	return nil
}

type providers []string

func (p *providers) String() string {
	return fmt.Sprintf("%v", *p)
}

func (p *providers) Set(value string) error {
	if value == "" {
		return errors.New("Cannot set blank value for RootFS provider")
	}

	*p = append(*p, value)
	return nil
}

const (
	dropsondeDestination = "localhost:3457"
	dropsondeOrigin      = "rep"
)

func main() {
	cf_debug_server.AddFlags(flag.CommandLine)
	cf_lager.AddFlags(flag.CommandLine)
	etcdFlags := etcdstoreadapter.AddFlags(flag.CommandLine)

	stackMap := stackPathMap{}
	supportedProviders := providers{}
	flag.Var(&stackMap, "preloadedRootFS", "List of preloaded RootFSes")
	flag.Var(&supportedProviders, "rootFSProvider", "List of RootFS providers")
	flag.Parse()

	cf_http.Initialize(*communicationTimeout)

	logger, reconfigurableSink := cf_lager.New(*sessionName)
	executorConfiguration := executorConfig()
	if !executorinit.ValidateExecutor(logger, executorConfiguration) {
		os.Exit(1)
	}
	initializeDropsonde(logger)

	etcdOptions, err := etcdFlags.Validate()
	if err != nil {
		logger.Fatal("etcd-validation-failed", err)
	}

	if *cellID == "" {
		log.Fatalf("-cellID must be specified")
	}

	executorClient, executorMembers, err := executorinit.Initialize(logger, executorConfiguration)
	if err != nil {
		log.Fatalf("Failed to initialize executor: %s", err.Error())
	}
	defer executorClient.Cleanup()

	if err := validateBBSAddress(); err != nil {
		logger.Fatal("invalid-bbs-address", err)
	}

	repBBS := initializeRepBBS(etcdOptions, logger)

	clock := clock.NewClock()

	evacuatable, evacuationReporter, evacuationNotifier := evacuation_context.New()

	// only one outstanding operation per container is necessary
	queue := operationq.NewSlidingQueue(1)

	bbsClient := bbs.NewClient(*bbsAddress)

	containerDelegate := internal.NewContainerDelegate(executorClient)
	lrpProcessor := internal.NewLRPProcessor(bbsClient, repBBS, containerDelegate, *cellID, evacuationReporter, uint64(evacuationTimeout.Seconds()))
	taskProcessor := internal.NewTaskProcessor(repBBS, containerDelegate, *cellID)

	evacuator := evacuation.NewEvacuator(
		logger,
		clock,
		executorClient,
		evacuationNotifier,
		*cellID,
		*evacuationTimeout,
		*evacuationPollingInterval,
	)

	httpServer, address := initializeServer(repBBS, executorClient, evacuatable, evacuationReporter, logger, rep.StackPathMap(stackMap), supportedProviders)
	opGenerator := generator.New(*cellID, bbsClient, repBBS, executorClient, lrpProcessor, taskProcessor, containerDelegate)

	preloadedRootFSes := []string{}
	for k := range stackMap {
		preloadedRootFSes = append(preloadedRootFSes, k)
	}

	members := grouper.Members{
		{"http_server", httpServer},
		{"presence", initializeCellPresence(address, repBBS, executorClient, logger, supportedProviders, preloadedRootFSes)},
		{"bulker", harmonizer.NewBulker(logger, *pollingInterval, *evacuationPollingInterval, evacuationNotifier, clock, opGenerator, queue)},
		{"event-consumer", harmonizer.NewEventConsumer(logger, opGenerator, queue)},
		{"evacuator", evacuator},
	}

	members = append(executorMembers, members...)

	if dbgAddr := cf_debug_server.DebugAddress(flag.CommandLine); dbgAddr != "" {
		members = append(grouper.Members{
			{"debug-server", cf_debug_server.Runner(dbgAddr, reconfigurableSink)},
		}, members...)
	}

	group := grouper.NewOrdered(os.Interrupt, members)

	monitor := ifrit.Invoke(sigmon.New(group))

	logger.Info("started", lager.Data{"cell-id": *cellID})

	err = <-monitor.Wait()
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

func initializeCellPresence(address string, repBBS legacybbs.RepBBS, executorClient executor.Client, logger lager.Logger, rootFSProviders, preloadedRootFSes []string) ifrit.Runner {
	config := maintain.Config{
		CellID:            *cellID,
		RepAddress:        address,
		Zone:              *zone,
		RetryInterval:     *lockRetryInterval,
		RootFSProviders:   rootFSProviders,
		PreloadedRootFSes: preloadedRootFSes,
	}
	return maintain.New(config, executorClient, repBBS, logger, clock.NewClock())
}

func initializeRepBBS(etcdOptions *etcdstoreadapter.ETCDOptions, logger lager.Logger) legacybbs.RepBBS {
	workPool, err := workpool.NewWorkPool(100)
	if err != nil {
		logger.Fatal("failed-to-construct-etcd-adapter-workpool", err, lager.Data{"num-workers": 100}) // should never happen
	}

	etcdAdapter, err := etcdstoreadapter.New(etcdOptions, workPool)

	if err != nil {
		logger.Fatal("failed-to-construct-etcd-tls-client", err)
	}

	client, err := consuladapter.NewClient(*consulCluster)
	if err != nil {
		logger.Fatal("new-client-failed", err)
	}

	sessionMgr := consuladapter.NewSessionManager(client)
	consulSession, err := consuladapter.NewSessionNoChecks(*sessionName, *lockTTL, client, sessionMgr)
	if err != nil {
		logger.Fatal("consul-session-failed", err)
	}

	return legacybbs.NewRepBBS(etcdAdapter, consulSession, *receptorTaskHandlerURL, clock.NewClock(), logger)
}

func initializeLRPStopper(guid string, executorClient executor.Client, logger lager.Logger) lrp_stopper.LRPStopper {
	return lrp_stopper.New(guid, executorClient, logger)
}

func initializeServer(
	repBBS legacybbs.RepBBS,
	executorClient executor.Client,
	evacuatable evacuation_context.Evacuatable,
	evacuationReporter evacuation_context.EvacuationReporter,
	logger lager.Logger,
	stackMap rep.StackPathMap,
	supportedProviders []string,
) (ifrit.Runner, string) {
	lrpStopper := initializeLRPStopper(*cellID, executorClient, logger)

	auctionCellRep := auction_cell_rep.New(*cellID, stackMap, supportedProviders, *zone, generateGuid, repBBS, executorClient, evacuationReporter, logger)
	handlers := auction_http_handlers.New(auctionCellRep, logger)

	routes := auctionroutes.Routes

	handlers[bbsroutes.StopLRPInstance] = repserver.NewStopLRPInstanceHandler(logger, lrpStopper)
	routes = append(routes, bbsroutes.StopLRPRoutes...)

	handlers[bbsroutes.CancelTask] = repserver.NewCancelTaskHandler(logger, executorClient)
	routes = append(routes, bbsroutes.CancelTaskRoutes...)

	handlers["Ping"] = repserver.NewPingHandler()
	routes = append(routes, rata.Route{Name: "Ping", Method: "GET", Path: "/ping"})

	handlers["Evacuate"] = repserver.NewEvacuationHandler(logger, evacuatable)
	routes = append(routes, rata.Route{Name: "Evacuate", Method: "POST", Path: "/evacuate"})

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

func validateBBSAddress() error {
	if *bbsAddress == "" {
		return errors.New("bbsAddress is required")
	}
	return nil
}

func generateGuid() (string, error) {
	guid, err := uuid.NewV4()
	if err != nil {
		return "", err
	}
	return guid.String(), nil
}
