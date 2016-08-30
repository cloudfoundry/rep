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
	binPath    string
	Session    *gexec.Session
	StartCheck string
	config     Config
}

type Config struct {
	PreloadedRootFSes   []string
	RootFSProviders     []string
	PlacementTags       []string
	CACertsForDownloads string
	CellID              string
	BBSAddress          string
	ServerPort          int
	GardenAddr          string
	LogLevel            string
	ConsulCluster       string
	PollingInterval     time.Duration
	EvacuationTimeout   time.Duration
}

func New(binPath string, config Config) *Runner {
	return &Runner{
		binPath:    binPath,
		StartCheck: "rep.started",
		config:     config,
	}
}

func (r *Runner) Start() {
	if r.Session != nil && r.Session.ExitCode() == -1 {
		panic("starting more than one rep!!!")
	}

	args := []string{
		"-cellID", r.config.CellID,
		"-listenAddr", fmt.Sprintf("0.0.0.0:%d", r.config.ServerPort),
		"-bbsAddress", r.config.BBSAddress,
		"-logLevel", r.config.LogLevel,
		"-pollingInterval", r.config.PollingInterval.String(),
		"-evacuationTimeout", r.config.EvacuationTimeout.String(),
		"-lockRetryInterval", "1s",
		"-consulCluster", r.config.ConsulCluster,
		"-containerMaxCpuShares", "1024",
		"-gardenNetwork", "tcp",
		"-gardenAddr", r.config.GardenAddr,
		"-gardenHealthcheckProcessUser", "me",
		"-gardenHealthcheckProcessPath", "ls",
	}
	for _, rootfs := range r.config.PreloadedRootFSes {
		args = append(args, "-preloadedRootFS", rootfs)
	}
	for _, provider := range r.config.RootFSProviders {
		args = append(args, "-rootFSProvider", provider)
	}
	for _, tag := range r.config.PlacementTags {
		args = append(args, "-placementTag", tag)
	}
	if r.config.CACertsForDownloads != "" {
		args = append(args, "-caCertsForDownloads", r.config.CACertsForDownloads)
	}

	repSession, err := gexec.Start(
		exec.Command(
			r.binPath,
			args...,
		),
		gexec.NewPrefixedWriter("\x1b[32m[o]\x1b[32m[rep]\x1b[0m ", ginkgo.GinkgoWriter),
		gexec.NewPrefixedWriter("\x1b[91m[e]\x1b[32m[rep]\x1b[0m ", ginkgo.GinkgoWriter),
	)

	Expect(err).NotTo(HaveOccurred())
	r.Session = repSession

	Eventually(r.Session.Buffer(), 2).Should(gbytes.Say(r.StartCheck))
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
