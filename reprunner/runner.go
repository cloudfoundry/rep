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
	stack       string
	listenAddr  string
	executorURL string
	etcdCluster string
	logLevel    string
}

func New(binPath, stack, listenAddr, executorURL, etcdCluster, logLevel string) *Runner {
	return &Runner{
		binPath: binPath,
		config: Config{
			stack:       stack,
			listenAddr:  listenAddr,
			executorURL: executorURL,
			etcdCluster: etcdCluster,
			logLevel:    logLevel,
		},
	}
}

func (r *Runner) Start() {
	if r.Session != nil {
		panic("starting more than one rep!!!")
	}

	repSession, err := gexec.Start(
		exec.Command(
			r.binPath,
			"-stack", r.config.stack,
			"-listenAddr", r.config.listenAddr,
			"-executorURL", r.config.executorURL,
			"-etcdCluster", r.config.etcdCluster,
			"-logLevel", r.config.logLevel,
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
		r.Session = nil
	}
}

func (r *Runner) KillWithFire() {
	if r.Session != nil {
		r.Session.Kill().Wait()
		r.Session = nil
	}
}
