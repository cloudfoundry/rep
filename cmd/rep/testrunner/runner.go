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
	stack                    string
	cellID                   string
	lrpHost                  string
	executorURL              string
	etcdCluster              string
	auctionServerPort        int
	logLevel                 string
	heartbeatInterval        time.Duration
	actualLRPReapingInterval time.Duration
	taskReapingInterval      time.Duration
}

func New(
	binPath, cellID, stack, lrpHost, executorURL, etcdCluster, logLevel string,
	auctionServerPort int,
	heartbeatInterval, actualLRPReapingInterval, taskReapingInterval time.Duration) *Runner {
	return &Runner{
		binPath: binPath,
		config: Config{
			cellID:                   cellID,
			stack:                    stack,
			lrpHost:                  lrpHost,
			executorURL:              executorURL,
			auctionServerPort:        auctionServerPort,
			etcdCluster:              etcdCluster,
			logLevel:                 logLevel,
			heartbeatInterval:        heartbeatInterval,
			actualLRPReapingInterval: actualLRPReapingInterval,
			taskReapingInterval:      taskReapingInterval,
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
			"-stack", r.config.stack,
			"-lrpHost", r.config.lrpHost,
			"-executorURL", r.config.executorURL,
			"-auctionListenAddr", fmt.Sprintf("0.0.0.0:%d", r.config.auctionServerPort),
			"-etcdCluster", r.config.etcdCluster,
			"-logLevel", r.config.logLevel,
			"-heartbeatInterval", r.config.heartbeatInterval.String(),
			"-actualLRPReapingInterval", r.config.actualLRPReapingInterval.String(),
			"-taskReapingInterval", r.config.taskReapingInterval.String(),
		),
		gexec.NewPrefixedWriter("\x1b[32m[o]\x1b[32m[rep]\x1b[0m ", ginkgo.GinkgoWriter),
		gexec.NewPrefixedWriter("\x1b[91m[e]\x1b[32m[rep]\x1b[0m ", ginkgo.GinkgoWriter),
	)

	Ω(err).ShouldNot(HaveOccurred())
	r.Session = repSession

	Eventually(r.Session.Buffer()).Should(gbytes.Say("rep.started"))
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
