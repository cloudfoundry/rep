package testrunner

import (
	"fmt"
	"os/exec"
	"time"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
)

type Runner struct {
	binPath string
	Session *gexec.Session
	config  Config
}

type Config struct {
	preloadedRootFSes string
	rootFSProviders   string
	cellID            string
	executorURL       string
	etcdCluster       string
	serverPort        int
	logLevel          string
	heartbeatInterval time.Duration
	pollingInterval   time.Duration
	evacuationTimeout time.Duration
}

func New(
	binPath, cellID, preloadedRootFSes, rootFSProviders, executorURL, etcdCluster, logLevel string,
	serverPort int,
	heartbeatInterval, pollingInterval, evacuationTimeout time.Duration) *Runner {
	return &Runner{
		binPath: binPath,
		config: Config{
			cellID:            cellID,
			preloadedRootFSes: preloadedRootFSes,
			rootFSProviders:   rootFSProviders,
			executorURL:       executorURL,
			serverPort:        serverPort,
			etcdCluster:       etcdCluster,
			logLevel:          logLevel,
			heartbeatInterval: heartbeatInterval,
			pollingInterval:   pollingInterval,
			evacuationTimeout: evacuationTimeout,
		},
	}
}

func (r *Runner) Start() {
	if r.Session != nil && r.Session.ExitCode() == -1 {
		panic("starting more than one rep!!!")
	}

	repSession, err := gexec.Start(
		exec.Command(
			r.binPath,
			"-cellID", r.config.cellID,
			"-preloadedRootFSes", r.config.preloadedRootFSes,
			"-rootFSProviders", r.config.rootFSProviders,
			"-executorURL", r.config.executorURL,
			"-listenAddr", fmt.Sprintf("0.0.0.0:%d", r.config.serverPort),
			"-etcdCluster", r.config.etcdCluster,
			"-logLevel", r.config.logLevel,
			"-heartbeatInterval", r.config.heartbeatInterval.String(),
			"-pollingInterval", r.config.pollingInterval.String(),
			"-evacuationTimeout", r.config.evacuationTimeout.String(),
		),
		gexec.NewPrefixedWriter("\x1b[32m[o]\x1b[32m[rep]\x1b[0m ", ginkgo.GinkgoWriter),
		gexec.NewPrefixedWriter("\x1b[91m[e]\x1b[32m[rep]\x1b[0m ", ginkgo.GinkgoWriter),
	)

	Ω(err).ShouldNot(HaveOccurred())
	r.Session = repSession

	Eventually(r.Session.Buffer(), 2).Should(gbytes.Say("rep.started"))
}

func (r *Runner) Stop() {
	if r.Session != nil {
		r.Session.Interrupt().Wait(5 * time.Second)
	}
}

func (r *Runner) KillWithFire() {
	if r.Session != nil {
		r.Session.Kill().Wait(5 * time.Second)
	}
}
