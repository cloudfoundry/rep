package reprunner

import (
	"os/exec"
	"syscall"
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
	listenAddr  string
	executorURL string
	etcdCluster string
	logLevel    string
}

func New(binPath, listenAddr, executorURL, etcdCluster, logLevel string) *Runner {
	return &Runner{
		binPath: binPath,
		config: Config{
			listenAddr:  listenAddr,
			executorURL: executorURL,
			etcdCluster: etcdCluster,
			logLevel:    logLevel,
		},
	}
}

func (r *Runner) Start(convergenceInterval, timeToClaim time.Duration) {
	convergerSession, err := gexec.Start(
		exec.Command(
			r.binPath,
			"-listenAddr", r.config.listenAddr,
			"-executorURL", r.config.executorURL,
			"-etcdCluster", r.config.etcdCluster,
			"-logLevel", r.config.logLevel,
		),
		GinkgoWriter,
		GinkgoWriter,
	)

	Ω(err).ShouldNot(HaveOccurred())
	r.Session = convergerSession
	Eventually(r.Session.Buffer()).Should(gbytes.Say("started"))
}

func (r *Runner) Stop() {
	if r.Session != nil {
		r.Session.Command.Process.Signal(syscall.SIGTERM)
		r.Session.Wait(5 * time.Second)
	}
}

func (r *Runner) KillWithFire() {
	if r.Session != nil {
		r.Session.Command.Process.Kill()
	}
}
