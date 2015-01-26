package evacuation_test

import (
	"syscall"

	"github.com/cloudfoundry-incubator/rep/evacuation"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Evacuation", func() {
	var (
		process           ifrit.Process
		evacuator         *evacuation.Evacuator
		logger            *lagertest.TestLogger
		evacuationContext evacuation.EvacuationContext
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test")
		evacuator = evacuation.NewEvacuator(logger)
		evacuationContext = evacuator.EvacuationContext()
		process = ifrit.Invoke(evacuator)
	})

	Describe("Signal", func() {
		Context("SIGUSR1", func() {
			BeforeEach(func() {
				process.Signal(syscall.SIGUSR1)
			})

			It("flips the bit on the evacuationContext", func() {
				Eventually(evacuationContext.Evacuating).Should(BeTrue())
			})
		})

		Context("any other signal", func() {
			BeforeEach(func() {
				process.Signal(syscall.SIGINT)
			})

			It("does not flip the bit on the evacuationContext", func() {
				Consistently(evacuationContext.Evacuating).Should(BeFalse())
			})
		})
	})
})
