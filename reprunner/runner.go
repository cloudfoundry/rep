package reprunner

import (
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo"
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
	lrpHost           string
	listenAddr        string
	executorURL       string
	etcdCluster       string
	logLevel          string
	heartbeatInterval time.Duration
}

func New(binPath, stack, lrpHost, listenAddr, executorURL, etcdCluster, logLevel string, heartbeatInterval time.Duration) *Runner {
	return &Runner{
		binPath: binPath,
		config: Config{
			stack:             stack,
			lrpHost:           lrpHost,
			listenAddr:        listenAddr,
			executorURL:       executorURL,
			etcdCluster:       etcdCluster,
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
			"-stack", r.config.stack,
			"-lrpHost", r.config.lrpHost,
			"-listenAddr", r.config.listenAddr,
			"-executorURL", r.config.executorURL,
			"-etcdCluster", r.config.etcdCluster,
			"-logLevel", r.config.logLevel,
			"-heartbeatInterval", r.config.heartbeatInterval.String(),
		),
		GinkgoWriter,
		GinkgoWriter,
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
		r.Session.Kill().Wait()
	}
}
