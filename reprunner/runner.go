package reprunner

import (
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
	stack             string
	repID             string
	lrpHost           string
	listenAddr        string
	executorURL       string
	etcdCluster       string
	natsAddr          string
	logLevel          string
	heartbeatInterval time.Duration
}

func New(binPath, repID, stack, lrpHost, listenAddr, executorURL, etcdCluster, natsAddr, logLevel string, heartbeatInterval time.Duration) *Runner {
	return &Runner{
		binPath: binPath,
		config: Config{
			repID:             repID,
			stack:             stack,
			lrpHost:           lrpHost,
			listenAddr:        listenAddr,
			executorURL:       executorURL,
			etcdCluster:       etcdCluster,
			natsAddr:          natsAddr,
			logLevel:          logLevel,
			heartbeatInterval: heartbeatInterval,
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
			"-repID", r.config.repID,
			"-stack", r.config.stack,
			"-lrpHost", r.config.lrpHost,
			"-listenAddr", r.config.listenAddr,
			"-executorURL", r.config.executorURL,
			"-etcdCluster", r.config.etcdCluster,
			"-natsAddresses", r.config.natsAddr,
			"-logLevel", r.config.logLevel,
			"-heartbeatInterval", r.config.heartbeatInterval.String(),
		),
		gexec.NewPrefixedWriter("\x1b[32m[o]\x1b[32m[rep]\x1b[0m ", ginkgo.GinkgoWriter),
		gexec.NewPrefixedWriter("\x1b[91m[e]\x1b[32m[rep]\x1b[0m ", ginkgo.GinkgoWriter),
	)

	Ω(err).ShouldNot(HaveOccurred())
	r.Session = repSession
	Eventually(r.Session.Buffer()).Should(gbytes.Say("started"))
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
