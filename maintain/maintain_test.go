package maintain_test

import (
	"errors"
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	fake_client "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep/maintain"
	maintain_fakes "github.com/cloudfoundry-incubator/rep/maintain/fakes"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/pivotal-golang/clock/fakeclock"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("Maintain Presence", func() {
	var (
		config          maintain.Config
		fakeHeartbeater *maintain_fakes.FakeRunner
		fakeClient      *fake_client.FakeClient
		fakeBBS         *fake_bbs.FakeRepBBS
		logger          *lagertest.TestLogger

		maintainer        ifrit.Runner
		maintainProcess   ifrit.Process
		heartbeaterErrors chan error
		observedSignals   chan os.Signal
		clock             *fakeclock.FakeClock
		pingErrors        chan error
	)

	BeforeEach(func() {
		pingErrors = make(chan error, 1)
		fakeClient = &fake_client.FakeClient{
			PingStub: func() error {
				return <-pingErrors
			},
		}
		resources := executor.ExecutorResources{MemoryMB: 128, DiskMB: 1024, Containers: 6}
		fakeClient.TotalResourcesReturns(resources, nil)

		logger = lagertest.NewTestLogger("test")
		clock = fakeclock.NewFakeClock(time.Now())

		heartbeaterErrors = make(chan error)
		observedSignals = make(chan os.Signal, 2)
		fakeHeartbeater = &maintain_fakes.FakeRunner{
			RunStub: func(sigChan <-chan os.Signal, ready chan<- struct{}) error {
				defer GinkgoRecover()
				logger.Info("fake-heartbeat-started")
				close(ready)
				for {
					select {
					case sig := <-sigChan:
						logger.Info("fake-heartbeat-received-signal")
						Eventually(observedSignals, time.Millisecond).Should(BeSent(sig))
						return nil
					case err := <-heartbeaterErrors:
						logger.Info("fake-heartbeat-received-error")
						return err
					}
				}
			},
		}

		fakeBBS = &fake_bbs.FakeRepBBS{}
		fakeBBS.NewCellPresenceReturns(fakeHeartbeater)

		config = maintain.Config{
			CellID:          "cell-id",
			RepAddress:      "1.2.3.4",
			Zone:            "az1",
			RetryInterval:   1 * time.Second,
			RootFSProviders: []string{"provider-1", "provider-2"},
		}
		maintainer = maintain.New(config, fakeClient, fakeBBS, logger, clock)
	})

	AfterEach(func() {
		logger.Info("test-complete-signaling-maintainer-to-stop")
		close(pingErrors)
		ginkgomon.Interrupt(maintainProcess)
	})

	It("pings the executor", func() {
		pingErrors <- nil
		maintainProcess = ginkgomon.Invoke(maintainer)
		Expect(fakeClient.PingCallCount()).To(Equal(1))
	})

	Context("when pinging the executor fails", func() {
		It("keeps pinging until it succeeds, then starts heartbeating the executor's presence", func() {
			maintainProcess = ifrit.Background(maintainer)
			ready := maintainProcess.Ready()

			for i := 1; i <= 4; i++ {
				clock.Increment(1 * time.Second)
				pingErrors <- errors.New("ping failed")
				Eventually(fakeClient.PingCallCount).Should(Equal(i))
				Expect(ready).NotTo(BeClosed())
			}

			pingErrors <- nil
			clock.Increment(1 * time.Second)
			Eventually(fakeClient.PingCallCount).Should(Equal(5))

			Eventually(ready).Should(BeClosed())
			Expect(fakeHeartbeater.RunCallCount()).To(Equal(1))
		})
	})

	Context("when pinging the executor succeeds", func() {
		Context("when the heartbeater is not ready", func() {
			BeforeEach(func() {
				fakeHeartbeater = &maintain_fakes.FakeRunner{
					RunStub: func(sigChan <-chan os.Signal, ready chan<- struct{}) error {
						defer GinkgoRecover()
						for {
							select {
							case sig := <-sigChan:
								logger.Info("never-ready-fake-heartbeat-received-signal")
								Eventually(observedSignals, time.Millisecond).Should(BeSent(sig))
								return nil
							case err := <-heartbeaterErrors:
								logger.Info("never-ready-fake-heartbeat-received-error")
								return err
							}
						}
					},
				}

				fakeBBS.NewCellPresenceReturns(fakeHeartbeater)

				pingErrors <- nil
				maintainProcess = ifrit.Background(maintainer)
			})

			It("exits when signaled", func() {
				maintainProcess.Signal(os.Interrupt)
				var err error
				Eventually(maintainProcess.Wait()).Should(Receive(&err))
				Expect(err).NotTo(HaveOccurred())
				Expect(maintainProcess.Ready()).NotTo(BeClosed())
			})

			Context("when the heartbeat errors", func() {
				BeforeEach(func() {
					heartbeaterErrors <- errors.New("oh no")
					pingErrors <- nil
				})

				It("does not shut down", func() {
					Consistently(maintainProcess.Wait()).ShouldNot(Receive(), "should not shut down")
				})

				It("retries to heartbeat", func() {
					Eventually(fakeBBS.NewCellPresenceCallCount).Should(Equal(2))
					Eventually(fakeHeartbeater.RunCallCount).Should(Equal(2))
				})
			})
		})

		Context("when the heartbeater is ready", func() {
			BeforeEach(func() {
				pingErrors <- nil
				maintainProcess = ginkgomon.Invoke(maintainer)
				Expect(maintainProcess.Ready()).To(BeClosed())
			})

			It("starts maintaining presence", func() {
				Expect(fakeBBS.NewCellPresenceCallCount()).To(Equal(1))
				Eventually(fakeHeartbeater.RunCallCount).Should(Equal(1))
			})

			It("continues pings the executor on an interval", func() {
				for i := 1; i < 5; i++ {
					pingErrors <- nil
					clock.Increment(1 * time.Second)
					Eventually(fakeClient.PingCallCount).Should(Equal(i))
				}
			})

			Context("when the executor ping fails", func() {
				BeforeEach(func() {
					pingErrors <- errors.New("failed to ping")
					clock.Increment(1 * time.Second)
				})

				It("stops heartbeating the executor's presence", func() {
					Eventually(observedSignals).Should(Receive(Equal(os.Kill)))
				})

				It("continues pinging the executor", func() {
					for i := 2; i < 6; i++ {
						pingErrors <- errors.New("failed again")
						clock.Increment(1 * time.Second)
						Eventually(fakeClient.PingCallCount).Should(Equal(i))
					}
				})

				Context("when the executor ping succeeds again", func() {
					BeforeEach(func() {
						pingErrors <- nil
						Eventually(fakeClient.PingCallCount).Should(Equal(3))
						pingErrors <- nil
						clock.Increment(1 * time.Second)
						Eventually(fakeClient.PingCallCount).Should(Equal(4))
					})

					It("begins heartbeating the executor's presence again", func() {
						Eventually(fakeHeartbeater.RunCallCount, 10*config.RetryInterval).Should(Equal(2))
					})

					It("continues to ping the executor", func() {
						for i := 4; i < 6; i++ {
							pingErrors <- nil
							clock.Increment(1 * time.Second)
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
						Eventually(fakeClient.PingCallCount).Should(Equal(i))
						clock.Increment(1 * time.Second)
					}
				})

				It("logs an error message", func() {
					Eventually(logger.TestSink.Buffer).Should(gbytes.Say("lost-lock"))
				})

				It("tries to restart heartbeating each time the ping succeeds", func() {
					Expect(fakeHeartbeater.RunCallCount()).To(Equal(1))

					Eventually(logger.TestSink.Buffer).Should(gbytes.Say("lost-lock"))

					pingErrors <- nil
					clock.Increment(1 * time.Second)

					Eventually(fakeHeartbeater.RunCallCount).Should(Equal(2))
				})
			})
		})
	})
})
