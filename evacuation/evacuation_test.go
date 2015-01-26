package evacuation_test

import (
	"syscall"
	"time"

	"github.com/cloudfoundry-incubator/rep/evacuation"
	"github.com/pivotal-golang/clock/fakeclock"
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
		evacuationTimeout time.Duration
		fakeClock         *fakeclock.FakeClock
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test")
		fakeClock = fakeclock.NewFakeClock(time.Now())
		evacuationTimeout = 3 * time.Minute
		evacuator = evacuation.NewEvacuator(logger, fakeClock, evacuationTimeout)
		evacuationContext = evacuator.EvacuationContext()
		process = ifrit.Invoke(evacuator)
	})

	Describe("Signal", func() {
		Context("SIGUSR1", func() {
			It("flips the bit on the evacuationContext", func() {
				process.Signal(syscall.SIGUSR1)
				Eventually(evacuationContext.Evacuating).Should(BeTrue())
			})

			It("exits after the evacuationTimeout has elapsed", func() {
				exitedCh := make(chan struct{})
				go func() {
					<-process.Wait()
					close(exitedCh)
				}()

				process.Signal(syscall.SIGUSR1)
				Eventually(fakeClock.WatcherCount).Should(Equal(1))

				fakeClock.IncrementBySeconds(179)
				Consistently(exitedCh).ShouldNot(BeClosed())
				fakeClock.IncrementBySeconds(2)
				Eventually(exitedCh).Should(BeClosed())
			})
		})

		Context("any other signal", func() {
			BeforeEach(func() {
				process.Signal(syscall.SIGINT)
			})

			It("does not flip the bit on the evacuationContext", func() {
				Consistently(evacuationContext.Evacuating).Should(BeFalse())
			})

			It("does not wait for evacuation before exiting", func() {
				wait := process.Wait()
				Eventually(wait).Should(Receive())
				Consistently(fakeClock.WatcherCount).Should(Equal(0))
			})
		})
	})
})
