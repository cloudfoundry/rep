package testrunner

import (
	"os/exec"
	"strconv"
	"time"

	"github.com/tedsuo/ifrit/ginkgomon"
)

func NewLDSRunner(ldsBinPath string, port int, listenerConfigPath string) *ginkgomon.Runner {

	return ginkgomon.New(ginkgomon.Config{
		Name:              "lds",
		StartCheck:        "lds.started",
		StartCheckTimeout: 10 * time.Second,
		Command:           exec.Command(ldsBinPath, "-port="+strconv.Itoa(port), "-listener-config="+listenerConfigPath),
	})

}
