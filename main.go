package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cloudfoundry-incubator/auction/auctionrep"
	"github.com/cloudfoundry-incubator/auction/communication/nats/repnatsserver"
	"github.com/cloudfoundry-incubator/executor/client"
	"github.com/cloudfoundry-incubator/rep/api"
	"github.com/cloudfoundry-incubator/rep/api/lrprunning"
	"github.com/cloudfoundry-incubator/rep/api/taskcomplete"
	"github.com/cloudfoundry-incubator/rep/auction_delegate"
	"github.com/cloudfoundry-incubator/rep/maintain"
	"github.com/cloudfoundry-incubator/rep/routes"
	"github.com/cloudfoundry-incubator/rep/task_scheduler"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	steno "github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"
	"github.com/cloudfoundry/storeadapter/workerpool"
	"github.com/cloudfoundry/yagnats"
	"github.com/nu7hatch/gouuid"
	"github.com/tedsuo/router"
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

var logLevel = flag.String(
	"logLevel",
	"info",
	"the logging level (none, fatal, error, warn, info, debug, debug1, debug2, all)",
)

var syslogName = flag.String(
	"syslogName",
	"",
	"syslog name",
)

var heartbeatInterval = flag.Duration(
	"heartbeatInterval",
	60*time.Second,
	"the interval, in seconds, between heartbeats for maintaining presence",
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

func main() {
	flag.Parse()
	if *stack == "" {
		log.Fatalf("-stack must be specified")
	}

	if *lrpHost == "" {
		log.Fatalf("-lrpHost must be specified")
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	repID := generateRepID()

	logger := initializeLogger()
	bbs := initializeRepBBS(logger)
	executorClient := client.New(http.DefaultClient, *executorURL)

	taskSchedulerReady := make(chan struct{})
	maintainReady := make(chan struct{})
	repNatsServerReady := make(chan struct{})

	go func() {
		<-maintainReady
		<-taskSchedulerReady
		<-repNatsServerReady

		fmt.Println("representative started")
	}()

	taskRep := initializeAndStartTaskRep(bbs, logger, executorClient, taskSchedulerReady)
	startHandlers(bbs, logger, executorClient)

	maintainSignals := make(chan os.Signal, 1)
	maintainPresence(repID, bbs, logger, maintainSignals, maintainReady)

	startAuctionNatsServer(repID, executorClient, logger, repNatsServerReady)

	for {
		sig := <-signals
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			taskRep.Stop()
			maintainSignals <- sig
			os.Exit(0)
		}
	}
}

func initializeLogger() *steno.Logger {
	l, err := steno.GetLogLevel(*logLevel)
	if err != nil {
		log.Fatalf("Invalid loglevel: %s\n", *logLevel)
	}

	stenoConfig := steno.Config{
		Level: l,
		Sinks: []steno.Sink{steno.NewIOSink(os.Stdout)},
	}

	if *syslogName != "" {
		stenoConfig.Sinks = append(stenoConfig.Sinks, steno.NewSyslogSink(*syslogName))
	}

	steno.Init(&stenoConfig)
	return steno.NewLogger("rep")
}

func initializeRepBBS(logger *steno.Logger) Bbs.RepBBS {
	etcdAdapter := etcdstoreadapter.NewETCDStoreAdapter(
		strings.Split(*etcdCluster, ","),
		workerpool.NewWorkerPool(10),
	)

	bbs := Bbs.NewRepBBS(etcdAdapter, timeprovider.NewTimeProvider())
	err := etcdAdapter.Connect()
	if err != nil {
		logger.Errord(map[string]interface{}{
			"error": err,
		}, "rep.etcd-connect.failed")
		os.Exit(1)
	}
	return bbs
}

func initializeAndStartTaskRep(bbs Bbs.RepBBS, logger *steno.Logger, executorClient client.Client, ready chan struct{}) *task_scheduler.TaskScheduler {
	callbackGenerator := router.NewRequestGenerator(
		"http://"+*listenAddr,
		routes.Routes,
	)

	taskRep := task_scheduler.New(callbackGenerator, bbs, logger, *stack, executorClient)

	err := taskRep.Run(ready)
	if err != nil {
		logger.Errord(map[string]interface{}{
			"error": err,
		}, "rep.task-scheduler.failed")
		os.Exit(1)
	}

	return taskRep
}

func generateRepID() string {
	uuid, err := uuid.NewV4()
	if err != nil {
		panic("Failed to generate a random guid....:" + err.Error())
	}
	return uuid.String()
}

func startHandlers(bbs Bbs.RepBBS, logger *steno.Logger, executorClient client.Client) {
	taskCompleteHandler := taskcomplete.NewHandler(bbs, logger)
	lrpRunningHandler := lrprunning.NewHandler(bbs, executorClient, *lrpHost, logger)

	apiHandler, err := api.NewServer(taskCompleteHandler, lrpRunningHandler)
	if err != nil {
		logger.Errord(map[string]interface{}{
			"error": err,
		}, "rep.api-server-initialize.failed")
		os.Exit(1)
	}

	apiListener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		logger.Errord(map[string]interface{}{
			"error": err,
		}, "rep.listening.failed")
		os.Exit(1)
	}

	go http.Serve(apiListener, apiHandler)
}

func maintainPresence(repID string, bbs Bbs.RepBBS, logger *steno.Logger, maintainSignals chan os.Signal, ready chan struct{}) {
	signal.Notify(maintainSignals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGUSR1)

	repPresence := models.RepPresence{
		RepID: repID,
		Stack: *stack,
	}
	maintainer := maintain.New(repPresence, bbs, logger, *heartbeatInterval)

	go func() {
		//keep maintaining forever, dont do anything if we fail to maintain
		for {
			err := maintainer.Run(maintainSignals, ready)
			if err != nil {
				logger.Errorf("failed to start maintaining presence: %s", err.Error())
				ready = make(chan struct{})
			} else {
				break
			}
		}
	}()
}

func initializeNatsClient(logger *steno.Logger) yagnats.NATSClient {
	natsClient := yagnats.NewClient()

	natsMembers := []yagnats.ConnectionProvider{}
	for _, addr := range strings.Split(*natsAddresses, ",") {
		natsMembers = append(
			natsMembers,
			&yagnats.ConnectionInfo{addr, *natsUsername, *natsPassword},
		)
	}

	err := natsClient.Connect(&yagnats.ConnectionCluster{
		Members: natsMembers,
	})

	if err != nil {
		logger.Fatalf("Error connecting to NATS: %s\n", err)
	}

	return natsClient
}

func startAuctionNatsServer(repID string, executorClient client.Client, logger *steno.Logger, ready chan struct{}) {
	auctionDelegate := auction_delegate.New(executorClient, logger)
	auctionRep := auctionrep.New(repID, auctionDelegate)
	natsClient := initializeNatsClient(logger)
	repnatsserver.Start(natsClient, auctionRep)
	close(ready)
}
