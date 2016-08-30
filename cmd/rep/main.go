package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"strings"
	"time"

	"code.cloudfoundry.org/bbs"
	"code.cloudfoundry.org/cfhttp"
	"code.cloudfoundry.org/cflager"
	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/consuladapter"
	"code.cloudfoundry.org/debugserver"
	"code.cloudfoundry.org/executor"
	executorinit "code.cloudfoundry.org/executor/initializer"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/localip"
	"code.cloudfoundry.org/operationq"
	"code.cloudfoundry.org/rep"
	"code.cloudfoundry.org/rep/auction_cell_rep"
	"code.cloudfoundry.org/rep/evacuation"
	"code.cloudfoundry.org/rep/evacuation/evacuation_context"
	"code.cloudfoundry.org/rep/generator"
	"code.cloudfoundry.org/rep/handlers"
	"code.cloudfoundry.org/rep/harmonizer"
	"code.cloudfoundry.org/rep/maintain"
	"github.com/cloudfoundry/dropsonde"
	"github.com/nu7hatch/gouuid"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/http_server"
	"github.com/tedsuo/ifrit/sigmon"
	"github.com/tedsuo/rata"
)

const (
	dropsondeOrigin = "rep"
	bbsPingTimeout  = 5 * time.Minute
)

func main() {
	debugserver.AddFlags(flag.CommandLine)
	cflager.AddFlags(flag.CommandLine)

	stackMap := stackPathMap{}
	flag.Var(&stackMap, "preloadedRootFS", "List of preloaded RootFSes")

	supportedProviders := multiArgList{}
	flag.Var(&supportedProviders, "rootFSProvider", "List of RootFS providers")

	gardenHealthcheckEnv := commaSeparatedArgList{}
	flag.Var(&gardenHealthcheckEnv, "gardenHealthcheckProcessEnv", "Environment variables to use when running the garden health check")

	gardenHealthcheckArgs := commaSeparatedArgList{}
	flag.Var(&gardenHealthcheckArgs, "gardenHealthcheckProcessArgs", "List of command line args to pass to the garden health check process")

	placementTags := multiArgList{}
	flag.Var(&placementTags, "placementTag", "Placement tags used for scheduling Tasks and LRPs")

	flag.Parse()

	preloadedRootFSes := []string{}
	for k := range stackMap {
		preloadedRootFSes = append(preloadedRootFSes, k)
	}

	cfhttp.Initialize(*communicationTimeout)

	clock := clock.NewClock()
	logger, reconfigurableSink := cflager.New(*sessionName)

	var (
		executorConfiguration   executorinit.Configuration
		gardenHealthcheckRootFS string
		certBytes               []byte
		err                     error
	)

	if len(preloadedRootFSes) == 0 {
		gardenHealthcheckRootFS = ""
	} else {
		gardenHealthcheckRootFS = stackMap[preloadedRootFSes[0]]
	}

	if *pathToCACertsForDownloads != "" {
		certBytes, err = ioutil.ReadFile(*pathToCACertsForDownloads)
		if err != nil {
			logger.Error("failed-to-open-ca-cert-file", err)
			os.Exit(1)
		}

		certBytes = bytes.TrimSpace(certBytes)
	}

	executorConfiguration = executorConfig(certBytes, gardenHealthcheckRootFS, gardenHealthcheckArgs, gardenHealthcheckEnv)
	if !executorConfiguration.Validate(logger) {
		logger.Fatal("", errors.New("failed-to-configure-executor"))
	}

	initializeDropsonde(logger)

	if *cellID == "" {
		logger.Error("invalid-cell-id", errors.New("-cellID must be specified"))
		os.Exit(1)
	}

	executorClient, executorMembers, err := executorinit.Initialize(logger, executorConfiguration, clock)
	if err != nil {
		logger.Error("failed-to-initialize-executor", err)
		os.Exit(1)
	}
	defer executorClient.Cleanup(logger)

	if err := validateBBSAddress(); err != nil {
		logger.Error("invalid-bbs-address", err)
		os.Exit(1)
	}

	serviceClient := initializeServiceClient(logger)

	evacuatable, evacuationReporter, evacuationNotifier := evacuation_context.New()

	// only one outstanding operation per container is necessary
	queue := operationq.NewSlidingQueue(1)

	evacuator := evacuation.NewEvacuator(
		logger,
		clock,
		executorClient,
		evacuationNotifier,
		*cellID,
		*evacuationTimeout,
		*evacuationPollingInterval,
	)

	bbsClient := initializeBBSClient(logger)
	httpServer, address := initializeServer(bbsClient, executorClient, evacuatable, evacuationReporter, logger, rep.StackPathMap(stackMap), supportedProviders)
	opGenerator := generator.New(*cellID, bbsClient, executorClient, evacuationReporter, uint64(evacuationTimeout.Seconds()))
	cleanup := evacuation.NewEvacuationCleanup(logger, *cellID, bbsClient, executorClient, clock)

	members := grouper.Members{
		{"presence", initializeCellPresence(address, serviceClient, executorClient, logger, supportedProviders, preloadedRootFSes, placementTags)},
		{"http_server", httpServer},
		{"evacuation-cleanup", cleanup},
		{"bulker", harmonizer.NewBulker(logger, *pollingInterval, *evacuationPollingInterval, evacuationNotifier, clock, opGenerator, queue)},
		{"event-consumer", harmonizer.NewEventConsumer(logger, opGenerator, queue)},
		{"evacuator", evacuator},
	}

	members = append(executorMembers, members...)

	if dbgAddr := debugserver.DebugAddress(flag.CommandLine); dbgAddr != "" {
		members = append(grouper.Members{
			{"debug-server", debugserver.Runner(dbgAddr, reconfigurableSink)},
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
	dropsondeDestination := fmt.Sprint("localhost:", *dropsondePort)
	err := dropsonde.Initialize(dropsondeDestination, dropsondeOrigin)
	if err != nil {
		logger.Error("failed to initialize dropsonde: %v", err)
	}
}

func initializeCellPresence(
	address string,
	serviceClient bbs.ServiceClient,
	executorClient executor.Client,
	logger lager.Logger,
	rootFSProviders,
	preloadedRootFSes,
	placementTags []string,
) ifrit.Runner {
	config := maintain.Config{
		CellID:            *cellID,
		RepAddress:        address,
		Zone:              *zone,
		RetryInterval:     *lockRetryInterval,
		RootFSProviders:   rootFSProviders,
		PreloadedRootFSes: preloadedRootFSes,
		PlacementTags:     placementTags,
	}
	return maintain.New(logger, config, executorClient, serviceClient, *lockTTL, clock.NewClock())
}

func initializeServiceClient(logger lager.Logger) bbs.ServiceClient {
	consulClient, err := consuladapter.NewClientFromUrl(*consulCluster)
	if err != nil {
		logger.Fatal("new-client-failed", err)
	}

	return bbs.NewServiceClient(consulClient, clock.NewClock())
}

func initializeServer(
	bbsClient bbs.InternalClient,
	executorClient executor.Client,
	evacuatable evacuation_context.Evacuatable,
	evacuationReporter evacuation_context.EvacuationReporter,
	logger lager.Logger,
	stackMap rep.StackPathMap,
	supportedProviders []string,
) (ifrit.Runner, string) {
	auctionCellRep := auction_cell_rep.New(*cellID, stackMap, supportedProviders, *zone, generateGuid, executorClient, evacuationReporter)
	handlers := handlers.New(auctionCellRep, executorClient, evacuatable, logger)

	router, err := rata.NewRouter(rep.Routes, handlers)
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

func initializeBBSClient(logger lager.Logger) bbs.InternalClient {
	bbsURL, err := url.Parse(*bbsAddress)
	if err != nil {
		logger.Fatal("Invalid BBS URL", err)
	}

	if bbsURL.Scheme != "https" {
		return bbs.NewClient(*bbsAddress)
	}

	bbsClient, err := bbs.NewSecureClient(*bbsAddress, *bbsCACert, *bbsClientCert, *bbsClientKey, *bbsClientSessionCacheSize, *bbsMaxIdleConnsPerHost)
	if err != nil {
		logger.Fatal("Failed to configure secure BBS client", err)
	}
	return bbsClient
}
