package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"code.cloudfoundry.org/bbs"
	"code.cloudfoundry.org/bbs/models"
	"code.cloudfoundry.org/cfhttp"
	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/consuladapter"
	"code.cloudfoundry.org/debugserver"
	loggingclient "code.cloudfoundry.org/diego-logging-client"
	"code.cloudfoundry.org/executor"
	executorinit "code.cloudfoundry.org/executor/initializer"
	"code.cloudfoundry.org/go-loggregator/runtimeemitter"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagerflags"
	"code.cloudfoundry.org/localip"
	"code.cloudfoundry.org/locket"
	"code.cloudfoundry.org/locket/lock"
	locketmodels "code.cloudfoundry.org/locket/models"
	"code.cloudfoundry.org/operationq"
	"code.cloudfoundry.org/rep"
	"code.cloudfoundry.org/rep/auctioncellrep"
	"code.cloudfoundry.org/rep/cmd/rep/config"
	"code.cloudfoundry.org/rep/evacuation"
	"code.cloudfoundry.org/rep/evacuation/evacuation_context"
	"code.cloudfoundry.org/rep/generator"
	"code.cloudfoundry.org/rep/handlers"
	"code.cloudfoundry.org/rep/harmonizer"
	"code.cloudfoundry.org/rep/maintain"
	"github.com/cloudfoundry/dropsonde"
	"github.com/hashicorp/consul/api"
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

var configFilePath = flag.String(
	"config",
	"",
	"The path to the JSON configuration file.",
)

var zoneOverride = flag.String(
	"zone",
	"",
	"The availability zone associated with the rep. This overrides the zone value in the config file, if specified.",
)

func main() {
	flag.Parse()

	repConfig, err := config.NewRepConfig(*configFilePath)
	if err != nil {
		panic(err.Error())
	}

	if *zoneOverride != "" {
		repConfig.Zone = *zoneOverride
	}

	cfhttp.Initialize(time.Duration(repConfig.CommunicationTimeout))

	clock := clock.NewClock()
	logger, reconfigurableSink := lagerflags.NewFromConfig(repConfig.SessionName, repConfig.LagerConfig)

	var gardenHealthcheckRootFS string

	rootFSes := repConfig.PreloadedRootFS
	rootFSNames := rootFSes.Names()
	if len(rootFSNames) > 0 {
		firstRootFS := rootFSNames[0]
		gardenHealthcheckRootFS = rootFSes.StackPathMap()[firstRootFS]
	}
	if !repConfig.ExecutorConfig.Validate(logger) {
		logger.Fatal("", errors.New("failed-to-configure-executor"))
	}

	if repConfig.CellID == "" {
		logger.Error("invalid-cell-id", errors.New("-cellID must be specified"))
		os.Exit(1)
	}

	metronClient, err := initializeMetron(logger, repConfig)
	if err != nil {
		logger.Error("failed-to-initialize-metron-client", err)
		os.Exit(1)
	}

	executorClient, executorMembers, err := executorinit.Initialize(logger, repConfig.ExecutorConfig, gardenHealthcheckRootFS, metronClient, clock)
	if err != nil {
		logger.Error("failed-to-initialize-executor", err)
		os.Exit(1)
	}
	defer executorClient.Cleanup(logger)

	consulClient := initializeConsulClient(logger, repConfig)

	serviceClient := maintain.NewCellPresenceClient(consulClient, clock)

	evacuatable, evacuationReporter, evacuationNotifier := evacuation_context.New()

	// only one outstanding operation per container is necessary
	queue := operationq.NewSlidingQueue(1)

	evacuator := evacuation.NewEvacuator(
		logger,
		clock,
		executorClient,
		evacuationNotifier,
		repConfig.CellID,
		time.Duration(repConfig.EvacuationTimeout),
		time.Duration(repConfig.EvacuationPollingInterval),
	)

	bbsClient := initializeBBSClient(logger, repConfig)
	url := repURL(repConfig)
	address := repAddress(logger, repConfig)
	cellPresence := initializeCellPresence(address, serviceClient, executorClient, logger, repConfig, rootFSNames, url)
	auctionCellRep := auctioncellrep.New(
		repConfig.CellID,
		url,
		rootFSes.StackPathMap(),
		repConfig.SupportedProviders,
		repConfig.Zone,
		auctioncellrep.GenerateGuid,
		executorClient,
		evacuationReporter,
		repConfig.PlacementTags,
		repConfig.OptionalPlacementTags,
	)
	httpServer := initializeServer(auctionCellRep, executorClient, evacuatable, logger, repConfig, false)
	httpsServer := initializeServer(auctionCellRep, executorClient, evacuatable, logger, repConfig, true)
	opGenerator := generator.New(
		repConfig.CellID,
		bbsClient,
		executorClient,
		evacuationReporter,
		uint64(time.Duration(repConfig.EvacuationTimeout).Seconds()),
	)
	cleanup := evacuation.NewEvacuationCleanup(logger, repConfig.CellID, bbsClient, executorClient, clock, metronClient)

	_, portString, err := net.SplitHostPort(repConfig.ListenAddr)
	if err != nil {
		logger.Fatal("failed-invalid-server-address", err)
	}
	portNum, err := net.LookupPort("tcp", portString)
	if err != nil {
		logger.Fatal("failed-invalid-server-port", err)
	}

	bulker := harmonizer.NewBulker(
		logger,
		time.Duration(repConfig.PollingInterval),
		time.Duration(repConfig.EvacuationPollingInterval),
		evacuationNotifier,
		clock,
		opGenerator,
		queue,
		metronClient,
	)

	members := grouper.Members{
		{"presence", cellPresence},
		{"http_server", httpServer},
		{"https_server", httpsServer},
		{"evacuation-cleanup", cleanup},
		{"bulker", bulker},
		{"event-consumer", harmonizer.NewEventConsumer(logger, opGenerator, queue)},
		{"evacuator", evacuator},
	}

	if repConfig.EnableConsulServiceRegistration {
		registrationRunner := initializeRegistrationRunner(logger, consulClient, repConfig, portNum, clock)
		members = append(members, grouper.Member{"registration-runner", registrationRunner})
	}

	members = append(executorMembers, members...)

	if repConfig.DebugAddress != "" {
		members = append(grouper.Members{
			{"debug-server", debugserver.Runner(repConfig.DebugAddress, reconfigurableSink)},
		}, members...)
	}

	group := grouper.NewOrdered(os.Interrupt, members)

	monitor := ifrit.Invoke(sigmon.New(group))

	logger.Info("started", lager.Data{"cell-id": repConfig.CellID})

	err = <-monitor.Wait()
	if err != nil {
		logger.Error("exited-with-failure", err)
		os.Exit(1)
	}

	logger.Info("exited")
}

func initializeDropsonde(logger lager.Logger, dropsondePort int) {
	dropsondeDestination := fmt.Sprint("localhost:", dropsondePort)
	err := dropsonde.Initialize(dropsondeDestination, dropsondeOrigin)
	if err != nil {
		logger.Error("failed to initialize dropsonde: %v", err)
	}
}

func initializeCellPresence(
	address string,
	serviceClient maintain.CellPresenceClient,
	executorClient executor.Client,
	logger lager.Logger,
	repConfig config.RepConfig,
	preloadedRootFSes []string,
	repUrl string,
) ifrit.Runner {
	if repConfig.LocketAddress != "" {
		locketClient, err := locket.NewClient(logger, repConfig.ClientLocketConfig)
		if err != nil {
			logger.Fatal("failed-to-construct-locket-client", err)
		}

		guid, err := uuid.NewV4()
		if err != nil {
			logger.Fatal("failed-to-generate-guid", err)
		}

		resources, err := executorClient.TotalResources(logger)
		if err != nil {
			logger.Fatal("failed-to-get-total-resources", err)
		}
		cellCapacity := models.NewCellCapacity(int32(resources.MemoryMB), int32(resources.DiskMB), int32(resources.Containers))
		cellPresence := models.NewCellPresence(repConfig.CellID, address, repUrl,
			repConfig.Zone, cellCapacity, repConfig.SupportedProviders,
			preloadedRootFSes, repConfig.PlacementTags, repConfig.OptionalPlacementTags)

		payload, err := json.Marshal(cellPresence)
		if err != nil {
			logger.Fatal("failed-to-encode-cell-presence", err)
		}

		lockPayload := &locketmodels.Resource{
			Key:      repConfig.CellID,
			Owner:    guid.String(),
			Value:    string(payload),
			TypeCode: locketmodels.PRESENCE,
			Type:     locketmodels.PresenceType,
		}

		logger.Debug("presence-payload", lager.Data{"payload": lockPayload})
		return lock.NewPresenceRunner(
			logger,
			locketClient,
			lockPayload,
			int64(time.Duration(repConfig.LockTTL)/time.Second),
			clock.NewClock(),
			locket.RetryInterval,
		)
	} else {
		config := maintain.Config{
			CellID:                repConfig.CellID,
			RepAddress:            address,
			RepUrl:                repUrl,
			Zone:                  repConfig.Zone,
			RetryInterval:         time.Duration(repConfig.LockRetryInterval),
			RootFSProviders:       repConfig.SupportedProviders,
			PreloadedRootFSes:     preloadedRootFSes,
			PlacementTags:         repConfig.PlacementTags,
			OptionalPlacementTags: repConfig.OptionalPlacementTags,
		}

		return maintain.New(
			logger,
			config,
			executorClient,
			serviceClient,
			time.Duration(repConfig.LockTTL),
			clock.NewClock(),
		)
	}
}

func initializeServer(
	auctionCellRep auctioncellrep.AuctionCellClient,
	executorClient executor.Client,
	evacuatable evacuation_context.Evacuatable,
	logger lager.Logger,
	repConfig config.RepConfig,
	secure bool,
) ifrit.Runner {
	handlers := getHandlers(logger, auctionCellRep, executorClient, evacuatable, repConfig.EnableLegacyAPIServer, secure)
	routes := getRoutes(repConfig.EnableLegacyAPIServer, secure)
	router, err := rata.NewRouter(routes, handlers)

	if err != nil {
		logger.Fatal("failed-to-construct-router", err)
	}

	listenAddress := repConfig.ListenAddr
	if secure {
		listenAddress = repConfig.ListenAddrSecurable
	}

	if secure && repConfig.RequireTLS {
		tlsConfig, err := cfhttp.NewTLSConfig(repConfig.ServerCertFile, repConfig.ServerKeyFile, repConfig.CaCertFile)
		if err != nil {
			logger.Fatal("tls-configuration-failed", err)
		}
		return http_server.NewTLSServer(listenAddress, router, tlsConfig)
	}

	return http_server.New(listenAddress, router)
}

func getHandlers(
	logger lager.Logger,
	auctionCellRep auctioncellrep.AuctionCellClient,
	executorClient executor.Client,
	evacuatable evacuation_context.Evacuatable,
	enableLegacyAPIServer bool,
	isSecureServer bool,
) rata.Handlers {

	if enableLegacyAPIServer && !isSecureServer {
		return handlers.NewLegacy(auctionCellRep, executorClient, evacuatable, logger)
	}
	return handlers.New(auctionCellRep, executorClient, evacuatable, logger, isSecureServer)
}

func getRoutes(enableLegacyAPIServer, isSecureServer bool) rata.Routes {
	if enableLegacyAPIServer && !isSecureServer {
		return rep.Routes
	}
	return rep.NewRoutes(isSecureServer)
}

func initializeBBSClient(
	logger lager.Logger,
	repConfig config.RepConfig,
) bbs.InternalClient {
	bbsURL, err := url.Parse(repConfig.BBSAddress)
	if err != nil {
		logger.Fatal("Invalid BBS URL", err)
	}

	if bbsURL.Scheme != "https" {
		return bbs.NewClient(repConfig.BBSAddress)
	}

	bbsClient, err := bbs.NewSecureClient(
		repConfig.BBSAddress,
		repConfig.BBSCACertFile,
		repConfig.BBSClientCertFile,
		repConfig.BBSClientKeyFile,
		repConfig.BBSClientSessionCacheSize,
		repConfig.BBSMaxIdleConnsPerHost,
	)
	if err != nil {
		logger.Fatal("Failed to configure secure BBS client", err)
	}
	return bbsClient
}

func initializeConsulClient(
	logger lager.Logger,
	repConfig config.RepConfig,
) consuladapter.Client {
	var consulClient consuladapter.Client

	scheme, _, err := consuladapter.Parse(repConfig.ConsulCluster)
	if err != nil {
		logger.Fatal("parse-consul-cluster-failed", err)
	}

	if scheme == "https" {
		consulClient, err = consuladapter.NewTLSClientFromUrl(
			repConfig.ConsulCluster,
			repConfig.ConsulCACert,
			repConfig.ConsulClientCert,
			repConfig.ConsulClientKey,
		)
	} else {
		consulClient, err = consuladapter.NewClientFromUrl(repConfig.ConsulCluster)
	}

	if err != nil {
		logger.Fatal("new-client-failed", err)
	}

	return consulClient
}

func repHost(cellID string) string {
	return strings.Replace(cellID, "_", "-", -1)
}

func repBaseHostName(advertiseDomain string) string {
	return strings.Split(advertiseDomain, ".")[0]
}

func repURL(config config.RepConfig) string {
	port := strings.Split(config.ListenAddrSecurable, ":")[1]
	if config.RequireTLS {
		return fmt.Sprintf("https://%s.%s:%s", repHost(config.CellID), config.AdvertiseDomain, port)
	}

	return fmt.Sprintf("http://%s.%s:%s", repHost(config.CellID), config.AdvertiseDomain, port)
}

func repAddress(logger lager.Logger, config config.RepConfig) string {
	ip, err := localip.LocalIP()
	if err != nil {
		logger.Fatal("failed-to-fetch-ip", err)
	}

	listenAddress := config.ListenAddr
	port := strings.Split(listenAddress, ":")[1]
	return fmt.Sprintf("http://%s:%s", ip, port)
}

func initializeRegistrationRunner(
	logger lager.Logger,
	consulClient consuladapter.Client,
	repConfig config.RepConfig,
	port int,
	clock clock.Clock,
) ifrit.Runner {
	registration := &api.AgentServiceRegistration{
		Name: repBaseHostName(repConfig.AdvertiseDomain),
		Port: port,
		Check: &api.AgentServiceCheck{
			TTL: "3s",
		},
		Tags: []string{repHost(repConfig.CellID)},
	}
	return locket.NewRegistrationRunner(logger, registration, consulClient, locket.RetryInterval, clock)
}

func initializeMetron(logger lager.Logger, repConfig config.RepConfig) (loggingclient.IngressClient, error) {
	client, err := loggingclient.NewIngressClient(repConfig.LoggregatorConfig)
	if err != nil {
		return nil, err
	}

	if repConfig.LoggregatorConfig.UseV2API {
		emitter := runtimeemitter.NewV1(client)
		go emitter.Run()
	} else {
		initializeDropsonde(logger, repConfig.DropsondePort)
	}

	return client, nil
}
