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
	preloadedRootFSes []string
	rootFSProviders   []string
	cellID            string
	executorURL       string
	etcdCluster       string
	serverPort        int
	logLevel          string
	consulCluster     string
	pollingInterval   time.Duration
	evacuationTimeout time.Duration
}

func New(
	binPath, cellID, executorURL, etcdCluster, consulCluster, logLevel string,
	preloadedRootFSes, rootFSProviders []string,
	serverPort int,
	pollingInterval, evacuationTimeout time.Duration) *Runner {
	return &Runner{
		binPath: binPath,
		config: Config{
			cellID:            cellID,
			preloadedRootFSes: preloadedRootFSes,
			rootFSProviders:   rootFSProviders,
			executorURL:       executorURL,
			serverPort:        serverPort,
			etcdCluster:       etcdCluster,
			consulCluster:     consulCluster,
			logLevel:          logLevel,
			pollingInterval:   pollingInterval,
			evacuationTimeout: evacuationTimeout,
		},
	}
}

func (r *Runner) Start() {
	if r.Session != nil && r.Session.ExitCode() == -1 {
		panic("starting more than one rep!!!")
	}

	args := []string{
		"-cellID", r.config.cellID,
		"-executorURL", r.config.executorURL,
		"-listenAddr", fmt.Sprintf("0.0.0.0:%d", r.config.serverPort),
		"-etcdCluster", r.config.etcdCluster,
		"-logLevel", r.config.logLevel,
		"-pollingInterval", r.config.pollingInterval.String(),
		"-evacuationTimeout", r.config.evacuationTimeout.String(),
		"-heartbeatRetryInterval", "1s",
		"-consulCluster", r.config.consulCluster,
	}
	for _, rootfs := range r.config.preloadedRootFSes {
		args = append(args, "-preloadedRootFS", rootfs)
	}
	for _, provider := range r.config.rootFSProviders {
		args = append(args, "-rootFSProvider", provider)
	}

	repSession, err := gexec.Start(
		exec.Command(
			r.binPath,
			args...,
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
