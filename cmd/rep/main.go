package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"code.cloudfoundry.org/bbs"
	"code.cloudfoundry.org/bbs/models"
	"code.cloudfoundry.org/cfhttp"
	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/debugserver"
	loggingclient "code.cloudfoundry.org/diego-logging-client"
	"code.cloudfoundry.org/executor"
	executorinit "code.cloudfoundry.org/executor/initializer"
	"code.cloudfoundry.org/go-loggregator/v8/runtimeemitter"
	"code.cloudfoundry.org/lager/v3"
	"code.cloudfoundry.org/lager/v3/lagerflags"
	"code.cloudfoundry.org/localip"
	"code.cloudfoundry.org/locket"
	"code.cloudfoundry.org/locket/lock"
	"code.cloudfoundry.org/locket/metrics/helpers"
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
	"code.cloudfoundry.org/tlsconfig"
	uuid "github.com/nu7hatch/gouuid"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/sigmon"
	"github.com/tedsuo/rata"
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

	// We need to keep this here since dockerdriver still uses cfhttp v1
	cfhttp.Initialize(time.Duration(repConfig.CommunicationTimeout))

	clock := clock.NewClock()
	logger, reconfigurableSink := lagerflags.NewFromConfig(repConfig.SessionName, repConfig.LagerConfig)

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

	rootFSMap := repConfig.PreloadedRootFS.StackPathMap()

	executorClient, containerMetricsProvider, executorMembers, err := executorinit.Initialize(logger, repConfig.ExecutorConfig, repConfig.CellID, repConfig.Zone, rootFSMap, metronClient, clock)
	if err != nil {
		logger.Error("failed-to-initialize-executor", err)
		os.Exit(1)
	}
	defer executorClient.Cleanup(logger)

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
	cellPresence := initializeCellPresence(address, executorClient, logger, repConfig, repConfig.PreloadedRootFS.Names(), url)
	batchContainerAllocator := auctioncellrep.NewContainerAllocator(auctioncellrep.GenerateGuid, rootFSMap, executorClient)
	auctionCellRep := auctioncellrep.New(
		repConfig.CellID,
		repConfig.CellIndex,
		url,
		rootFSMap,
		containerMetricsProvider,
		repConfig.SupportedProviders,
		repConfig.Zone,
		executorClient,
		evacuationReporter,
		repConfig.PlacementTags,
		repConfig.OptionalPlacementTags,
		repConfig.ProxyMemoryAllocationMB,
		repConfig.EnableContainerProxy,
		batchContainerAllocator,
	)

	requestTypes := []string{
		"State", "ContainerMetrics", "Perform", "Reset", "UpdateLRPInstance", "StopLRPInstance", "CancelTask", //over https only
	}
	requestMetrics := helpers.NewRequestMetricsNotifier(logger, clock, metronClient, time.Duration(repConfig.ReportInterval), requestTypes)
	httpServer := initializeServer(auctionCellRep, executorClient, evacuatable, requestMetrics, logger, repConfig, false)
	httpsServer := initializeServer(auctionCellRep, executorClient, evacuatable, requestMetrics, logger, repConfig, true)

	opGenerator := generator.New(
		repConfig.CellID,
		rootFSMap,
		repConfig.LayeringMode,
		bbsClient,
		executorClient,
		metronClient,
		evacuationReporter,
	)

	cleanup := evacuation.NewEvacuationCleanup(
		logger,
		repConfig.CellID,
		time.Duration(repConfig.GracefulShutdownInterval),
		time.Duration(repConfig.ExecutorConfig.EnvoyConfigReloadDuration),
		bbsClient,
		executorClient,
		clock,
		metronClient,
	)

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
		{"request-metrics-notifier", requestMetrics},
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

func initializeCellPresence(
	address string,
	executorClient executor.Client,
	logger lager.Logger,
	repConfig config.RepConfig,
	preloadedRootFSes []string,
	repUrl string,
) ifrit.Runner {
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
}

func initializeServer(
	auctionCellRep *auctioncellrep.AuctionCellRep,
	executorClient executor.Client,
	evacuatable evacuation_context.Evacuatable,
	requestMetrics helpers.RequestMetrics,
	logger lager.Logger,
	repConfig config.RepConfig,
	networkAccessible bool,
) ifrit.Runner {
	handlers := handlers.New(auctionCellRep, auctionCellRep, executorClient, evacuatable, requestMetrics, logger, networkAccessible)
	routes := rep.NewRoutes(networkAccessible)
	router, err := rata.NewRouter(routes, handlers)

	if err != nil {
		logger.Fatal("failed-to-construct-router", err)
	}

	listenAddress := repConfig.ListenAddr
	if networkAccessible {
		listenAddress = repConfig.ListenAddrSecurable
	}

	if !networkAccessible {
		err = verifyCertificate(repConfig.CertFile)
		if err != nil {
			logger.Fatal("tls-configuration-failed", err)
		}
	}

	tlsConfig, err := tlsconfig.Build(
		tlsconfig.WithInternalServiceDefaults(),
		tlsconfig.WithIdentityFromFile(repConfig.CertFile, repConfig.KeyFile),
	).Server(tlsconfig.WithClientAuthenticationFromFile(repConfig.CaCertFile))
	if err != nil {
		logger.Fatal("tls-configuration-failed", err)
	}
	return startTLSServer(listenAddress, router, tlsConfig)
}

func startTLSServer(addr string, handler http.Handler, tlsConfig *tls.Config) ifrit.Runner {
	return ifrit.RunFunc(func(signals <-chan os.Signal, ready chan<- struct{}) error {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		listener = tls.NewListener(listener, tlsConfig)
		close(ready)
		go http.Serve(listener, handler)
		<-signals
		return listener.Close()
	})
}

func initializeBBSClient(
	logger lager.Logger,
	repConfig config.RepConfig,
) bbs.InternalClient {
	bbsClient, err := bbs.NewClientWithConfig(bbs.ClientConfig{
		URL:                    repConfig.BBSAddress,
		IsTLS:                  true,
		CAFile:                 repConfig.CaCertFile,
		CertFile:               repConfig.CertFile,
		KeyFile:                repConfig.KeyFile,
		ClientSessionCacheSize: repConfig.BBSClientSessionCacheSize,
		MaxIdleConnsPerHost:    repConfig.BBSMaxIdleConnsPerHost,
		RequestTimeout:         time.Duration(repConfig.CommunicationTimeout),
	})
	if err != nil {
		logger.Fatal("failed-to-configure-secure-BBS-client", err)
	}
	return bbsClient
}

func repHost(cellID string) string {
	return strings.Replace(cellID, "_", "-", -1)
}

func repBaseHostName(advertiseDomain string) string {
	return strings.Split(advertiseDomain, ".")[0]
}

func repURL(config config.RepConfig) string {
	port := strings.Split(config.ListenAddrSecurable, ":")[1]
	return fmt.Sprintf("https://%s.%s:%s", repHost(config.CellID), config.AdvertiseDomain, port)
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

func initializeMetron(logger lager.Logger, repConfig config.RepConfig) (loggingclient.IngressClient, error) {
	client, err := loggingclient.NewIngressClient(repConfig.LoggregatorConfig)
	if err != nil {
		return nil, err
	}

	if repConfig.LoggregatorConfig.UseV2API {
		emitter := runtimeemitter.NewV1(client)
		go emitter.Run()
	}

	return client, nil
}

func verifyCertificate(serverCertFile string) error {
	certBytes, err := ioutil.ReadFile(serverCertFile)
	if err != nil {
		return err
	}

	var blocks []byte
	certBytes = []byte(strings.TrimSpace(string(certBytes)))
	for {
		var block *pem.Block
		block, certBytes = pem.Decode(certBytes)
		if block == nil {
			return fmt.Errorf("failed parsing cert")
		}
		blocks = append(blocks, block.Bytes...)
		if len(certBytes) == 0 {
			break
		}
	}

	certs, err := x509.ParseCertificates(blocks)
	if err != nil {
		return fmt.Errorf("failed parsing cert: %s", err)
	}

	for _, ip := range certs[0].IPAddresses {
		if ip.Equal(net.ParseIP("127.0.0.1")) {
			return nil
		}
	}

	return errors.New("invalid SAN metadata. certificate needs to contain 127.0.0.1 for IP SAN metadata.")
}
