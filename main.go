package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/cloudfoundry-incubator/executor/client"
	"github.com/cloudfoundry-incubator/rep/lrp_scheduler"
	"github.com/cloudfoundry-incubator/rep/task_scheduler"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	steno "github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"
	"github.com/cloudfoundry/storeadapter/workerpool"
)

var etcdCluster = flag.String(
	"etcdCluster",
	"http://127.0.0.1:4001",
	"comma-separated list of etcd addresses (http://ip:port)",
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

var executorURL = flag.String(
	"executorURL",
	"http://127.0.0.1:1700",
	"location of executor to represent",
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

	l, err := steno.GetLogLevel(*logLevel)
	if err != nil {
		log.Fatalf("Invalid loglevel: %s\n", *logLevel)
	}

	if *stack == "" {
		log.Fatalf("A stack must be specified")
	}

	stenoConfig := steno.Config{
		Level: l,
		Sinks: []steno.Sink{steno.NewIOSink(os.Stdout)},
	}

	if *syslogName != "" {
		stenoConfig.Sinks = append(stenoConfig.Sinks, steno.NewSyslogSink(*syslogName))
	}

	steno.Init(&stenoConfig)
	logger := steno.NewLogger("rep")

	etcdAdapter := etcdstoreadapter.NewETCDStoreAdapter(
		strings.Split(*etcdCluster, ","),
		workerpool.NewWorkerPool(10),
	)

	bbs := Bbs.NewRepBBS(etcdAdapter, timeprovider.NewTimeProvider())
	err = etcdAdapter.Connect()
	if err != nil {
		logger.Errord(map[string]interface{}{
			"error": err,
		}, "rep.etcd-connect.failed")
		os.Exit(1)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	executorClient := client.New(http.DefaultClient, *executorURL)

	taskRep := task_scheduler.New(bbs, logger, *stack, *listenAddr, executorClient)
	lrpRep := lrp_scheduler.New(bbs, logger, *stack, executorClient)

	taskSchedulerReady := make(chan struct{})
	lrpSchedulerReady := make(chan struct{})

	go func() {
		<-taskSchedulerReady
		<-lrpSchedulerReady
		fmt.Println("representative started")
	}()

	err = taskRep.Run(taskSchedulerReady)
	if err != nil {
		logger.Errord(map[string]interface{}{
			"error": err,
		}, "rep.task-scheduler.failed")
		os.Exit(1)
	}

	lrpRep.Run(lrpSchedulerReady)

	for {
		sig := <-signals
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			taskRep.Stop()
			lrpRep.Stop()
			os.Exit(0)
		}
	}
}
