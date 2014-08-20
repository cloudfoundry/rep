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
	executorapi "github.com/cloudfoundry-incubator/executor/api"
	"github.com/cloudfoundry-incubator/executor/client"
	"github.com/cloudfoundry-incubator/rep/api"
	"github.com/cloudfoundry-incubator/rep/api/lrprunning"
	"github.com/cloudfoundry-incubator/rep/api/taskcomplete"
	"github.com/cloudfoundry-incubator/rep/auction_delegate"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
	"github.com/cloudfoundry-incubator/rep/maintain"
	"github.com/cloudfoundry-incubator/rep/routes"
	"github.com/cloudfoundry-incubator/rep/stop_lrp_listener"
	"github.com/cloudfoundry-incubator/rep/task_scheduler"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/cloudfoundry-incubator/runtime-schema/heartbeater"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gunk/group_runner"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"
	"github.com/cloudfoundry/storeadapter/workerpool"
	"github.com/cloudfoundry/yagnats"
	"github.com/nu7hatch/gouuid"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/timer"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/http_server"
	"github.com/tedsuo/ifrit/sigmon"
	"github.com/tedsuo/rata"
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

var executorID = flag.String(
	"executorID",
	"",
	"the ID used by the rep to identify itself to external systems - must be specified",
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
	store := initializeStore()
	bbs := initializeRepBBS(store, logger)
	executorClient := client.New(http.DefaultClient, *executorURL)
	lrpStopper := initializeLRPStopper(bbs, executorClient, logger)

	monitor := ifrit.Envoke(sigmon.New(group_runner.New([]group_runner.Member{
		{"maintainer", initializeMaintainer(*executorID, executorClient, store, logger)},
		{"task-rep", initializeTaskRep(*executorID, bbs, logger, executorClient)},
		{"stop-lrp-listener", initializeStopLRPListener(lrpStopper, bbs, logger)},
		{"api-server", initializeAPIServer(*executorID, bbs, logger, executorClient)},
		{"auction-server", initializeAuctionNatsServer(*executorID, lrpStopper, bbs, executorClient, logger)},
	})))

	logger.Info("started")

	<-monitor.Wait()
	logger.Info("shutting-down")
}

func initializeStore() *etcdstoreadapter.ETCDStoreAdapter {
	return etcdstoreadapter.NewETCDStoreAdapter(
		strings.Split(*etcdCluster, ","),
		workerpool.NewWorkerPool(10),
	)
}

func initializeRepBBS(etcdAdapter *etcdstoreadapter.ETCDStoreAdapter, logger lager.Logger) Bbs.RepBBS {
	bbs := Bbs.NewRepBBS(etcdAdapter, timeprovider.NewTimeProvider(), logger)

	err := etcdAdapter.Connect()
	if err != nil {
		logger.Fatal("failed-to-connect-to-etcd", err)
		os.Exit(1)
	}

	return bbs
}

func initializeTaskRep(executorID string, bbs Bbs.RepBBS, logger lager.Logger, executorClient executorapi.Client) *task_scheduler.TaskScheduler {
	callbackGenerator := rata.NewRequestGenerator(
		"http://"+*listenAddr,
		routes.Routes,
	)

	return task_scheduler.New(executorID, callbackGenerator, bbs, logger, *stack, executorClient)
}

func generateExecutorID() string {
	uuid, err := uuid.NewV4()
	if err != nil {
		panic("Failed to generate a random guid....:" + err.Error())
	}
	return uuid.String()
}

func initializeLRPStopper(bbs Bbs.RepBBS, executorClient executorapi.Client, logger lager.Logger) lrp_stopper.LRPStopper {
	return lrp_stopper.New(bbs, executorClient, logger)
}

func initializeStopLRPListener(stopper lrp_stopper.LRPStopper, bbs Bbs.RepBBS, logger lager.Logger) ifrit.Runner {
	return stop_lrp_listener.New(stopper, bbs, logger)
}

func initializeAPIServer(executorID string, bbs Bbs.RepBBS, logger lager.Logger, executorClient executorapi.Client) ifrit.Runner {
	taskCompleteHandler := taskcomplete.NewHandler(bbs, executorClient, logger)
	lrpRunningHandler := lrprunning.NewHandler(executorID, bbs, executorClient, *lrpHost, logger)

	apiHandler, err := api.NewServer(taskCompleteHandler, lrpRunningHandler)
	if err != nil {
		panic("failed to initialize api server: " + err.Error())
	}
	return http_server.New(*listenAddr, apiHandler)
}

func initializeMaintainer(executorID string, executorClient executorapi.Client, etcdAdapter *etcdstoreadapter.ETCDStoreAdapter, logger lager.Logger) *maintain.Maintainer {
	executorPresence := models.ExecutorPresence{
		ExecutorID: executorID,
		Stack:      *stack,
	}

	heartbeater := heartbeater.New(
		etcdAdapter,
		shared.ExecutorSchemaPath(executorPresence.ExecutorID),
		string(executorPresence.ToJSON()),
		500*time.Millisecond,
		logger,
	)

	return maintain.New(executorClient, heartbeater, logger, *heartbeatInterval, timer.NewTimer())
}

func initializeNatsClient(logger lager.Logger) yagnats.NATSClient {
	natsClient := yagnats.NewClient()

	natsMembers := []yagnats.ConnectionProvider{}
	for _, addr := range strings.Split(*natsAddresses, ",") {
		natsMembers = append(
			natsMembers,
			&yagnats.ConnectionInfo{
				Addr:     addr,
				Username: *natsUsername,
				Password: *natsPassword,
			},
		)
	}

	err := natsClient.Connect(&yagnats.ConnectionCluster{
		Members: natsMembers,
	})

	if err != nil {
		logger.Fatal("failed-to-connect-to-nats", err)
	}

	return natsClient
}

func initializeAuctionNatsServer(executorID string, stopper lrp_stopper.LRPStopper, bbs Bbs.RepBBS, executorClient executorapi.Client, logger lager.Logger) *auction_nats_server.AuctionNATSServer {
	auctionDelegate := auction_delegate.New(executorID, stopper, bbs, executorClient, logger)
	auctionRep := auctionrep.New(executorID, auctionDelegate)
	natsClient := initializeNatsClient(logger)
	return auction_nats_server.New(natsClient, auctionRep, logger)
}
