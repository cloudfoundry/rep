package maintain_test

import (
	"errors"
	"os"
	"syscall"
	"time"

	fake_client "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep/maintain"
	maintain_fakes "github.com/cloudfoundry-incubator/rep/maintain/fakes"
	"github.com/cloudfoundry/gunk/timeprovider/faketimeprovider"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("Maintain Presence", func() {
	var (
		heartbeatInterval = 500 * time.Millisecond

		fakeHeartbeater *maintain_fakes.FakeRunner
		fakeClient      *fake_client.FakeClient
		logger          *lagertest.TestLogger

		maintainer        ifrit.Runner
		maintainProcess   ifrit.Process
		heartbeaterErrors chan error
		timeProvider      *faketimeprovider.FakeTimeProvider
		pingErrors        chan error
	)

	BeforeEach(func() {
		pingErrors = make(chan error, 1)
		fakeClient = &fake_client.FakeClient{
			PingStub: func() error {
				return <-pingErrors
			},
		}

		heartbeaterErrors = make(chan error)
		fakeHeartbeater = &maintain_fakes.FakeRunner{
			RunStub: func(sigChan <-chan os.Signal, ready chan<- struct{}) error {
				close(ready)
				return <-heartbeaterErrors
			},
		}

		logger = lagertest.NewTestLogger("test")
		timeProvider = faketimeprovider.New(time.Now())

		maintainer = maintain.New(fakeClient, fakeHeartbeater, logger, heartbeatInterval, timeProvider)
	})

	AfterEach(func() {
		maintainProcess.Signal(syscall.SIGTERM)
		close(heartbeaterErrors)
		Eventually(maintainProcess.Wait()).Should(Receive())
	})

	It("pings the executor", func() {
		pingErrors <- nil
		maintainProcess = ifrit.Envoke(maintainer)
		Ω(fakeClient.PingCallCount()).Should(Equal(1))
	})

	Context("when pinging the executor fails", func() {
		It("keeps pinging until it succeeds, then starts heartbeating the executor's presence", func() {
			ready := make(chan struct{})
			go func() {
				maintainProcess = ifrit.Envoke(maintainer)
				close(ready)
			}()

			for i := 1; i <= 4; i++ {
				timeProvider.Increment(1 * time.Second)
				pingErrors <- errors.New("ping failed")
				Eventually(fakeClient.PingCallCount).Should(Equal(i))
				Ω(ready).ShouldNot(BeClosed())
			}

			pingErrors <- nil
			timeProvider.Increment(1 * time.Second)
			Eventually(fakeClient.PingCallCount).Should(Equal(5))

			Eventually(ready).Should(BeClosed())
			Ω(fakeHeartbeater.RunCallCount()).Should(Equal(1))
		})
	})

	Context("when pinging the executor succeeds", func() {
		BeforeEach(func() {
			pingErrors <- nil

			maintainProcess = ifrit.Envoke(maintainer)
		})

		It("starts maintaining presence", func() {
			Ω(fakeHeartbeater.RunCallCount()).Should(Equal(1))
		})

		It("continues pings the executor on an interval", func() {
			for i := 1; i < 5; i++ {
				pingErrors <- nil
				timeProvider.Increment(1 * time.Second)
				Eventually(fakeClient.PingCallCount).Should(Equal(i))
			}
		})

		Context("when the executor ping fails", func() {
			BeforeEach(func() {
				pingErrors <- errors.New("failed to ping")
				timeProvider.Increment(1 * time.Second)
			})

			It("stops heartbeating the executor's presence", func() {
				sigChan, _ := fakeHeartbeater.RunArgsForCall(0)
				Eventually(sigChan).Should(Receive(Equal(os.Kill)))
			})

			It("continues pinging the executor", func() {
				for i := 2; i < 6; i++ {
					pingErrors <- errors.New("failed again")
					timeProvider.Increment(1 * time.Second)
					Eventually(fakeClient.PingCallCount).Should(Equal(i))
				}
			})

			Context("when the executor ping succeeds again", func() {
				BeforeEach(func() {
					heartbeaterErrors <- nil

					Eventually(fakeClient.PingCallCount).Should(Equal(2))

					pingErrors <- nil
					timeProvider.Increment(1 * time.Second)
					Eventually(fakeClient.PingCallCount).Should(Equal(3))
				})

				It("begins heartbeating the executor's presence again", func() {
					Eventually(fakeHeartbeater.RunCallCount, 10*heartbeatInterval).Should(Equal(2))
				})

				It("continues to ping the executor", func() {
					for i := 4; i < 6; i++ {
						pingErrors <- nil
						timeProvider.Increment(1 * time.Second)
						Eventually(fakeClient.PingCallCount).Should(Equal(i))
					}
				})
			})
		})

		Context("when heartbeating fails", func() {
			BeforeEach(func() {
				heartbeaterErrors <- errors.New("heartbeating failed")
			})

			It("does not shut down", func() {
				Consistently(maintainProcess.Wait()).ShouldNot(Receive(), "should not shut down")
			})

			It("continues pinging the executor", func() {
				for i := 2; i < 6; i++ {
					pingErrors <- nil
					timeProvider.Increment(1 * time.Second)
					Eventually(fakeClient.PingCallCount).Should(Equal(i))
				}
			})

			It("tries to restart heartbeating each time the ping succeeds", func() {
				Ω(fakeHeartbeater.RunCallCount()).Should(Equal(1))

				pingErrors <- nil
				timeProvider.Increment(1 * time.Second)

				Eventually(fakeHeartbeater.RunCallCount).Should(Equal(2))
			})

			It("logs an error message", func() {
				Eventually(logger.TestSink.Buffer).Should(gbytes.Say("lost-lock"))
			})
		})
	})
})
