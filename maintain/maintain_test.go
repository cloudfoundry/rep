package maintain_test

import (
	"errors"
	"syscall"
	"time"

	"github.com/cloudfoundry-incubator/executor/client/fake_client"
	"github.com/cloudfoundry-incubator/rep/maintain"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	steno "github.com/cloudfoundry/gosteno"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Maintain Presence", func() {
	var (
		executorPresence  models.ExecutorPresence
		heartbeatInterval = 1 * time.Second

		fakeBBS    *fake_bbs.FakeRepBBS
		fakeClient *fake_client.FakeClient
		logger     *steno.Logger

		maintainer ifrit.Process
		pings      chan bool

		presence           *fake_bbs.FakePresence
		maintainStatusChan chan bool
	)

	BeforeSuite(func() {
		steno.EnterTestMode(steno.LOG_DEBUG)
	})

	BeforeEach(func() {
		fakeClient = fake_client.New()
		pings = make(chan bool, 10)
		fakeClient.WhenPinging = func() error {
			pings <- true
			return nil
		}
		presence = &fake_bbs.FakePresence{}
		maintainStatusChan = make(chan bool)

		executorPresence = models.ExecutorPresence{
			ExecutorID: "executor-id",
			Stack:      "lucid64",
		}

		fakeBBS = &fake_bbs.FakeRepBBS{}
		fakeBBS.MaintainExecutorPresenceReturns(presence, maintainStatusChan, nil)

		logger = steno.NewLogger("test-logger")

		maintainer = ifrit.Envoke(maintain.New(executorPresence, fakeClient, fakeBBS, logger, heartbeatInterval))
	})

	AfterEach(func() {
		maintainer.Signal(syscall.SIGTERM)
		<-maintainer.Wait()
	})

	Context("when maintaining presence", func() {
		It("should maintain presence", func() {
			maintainStatusChan <- true
			maintainStatusChan <- true

			Eventually(fakeBBS.MaintainExecutorPresenceCallCount).Should(Equal(1))
			interval, maintainedPresence := fakeBBS.MaintainExecutorPresenceArgsForCall(0)
			Ω(interval).Should(Equal(heartbeatInterval))
			Ω(maintainedPresence).Should(Equal(executorPresence))
		})

		It("should ping the executor on each maintain tick", func() {
			maintainStatusChan <- true
			Eventually(pings).Should(Receive())

			maintainStatusChan <- true
			Eventually(pings).Should(Receive())
		})

		Context("when the executor ping fails", func() {
			BeforeEach(func() {
				maintainStatusChan <- true
				Eventually(pings).Should(Receive())
				Consistently(presence.Removed).Should(BeFalse())

				fakeClient.WhenPinging = func() error {
					pings <- true
					return errors.New("bam")
				}

				maintainStatusChan <- true
				Eventually(pings).Should(Receive())
			})

			It("should remove presence", func() {
				Eventually(presence.Removed).Should(BeTrue())
			})

			It("should ping the executor until it comes back, then reestablish presence", func(done Done) {
				newMaintainStatusChan := make(chan bool)
				fakeBBS.MaintainExecutorPresenceReturns(presence, newMaintainStatusChan, nil)

				//healthy again
				fakeClient.WhenPinging = func() error {
					pings <- true
					return nil
				}

				Eventually(pings, 5).Should(Receive())

				newMaintainStatusChan <- true
				Eventually(pings).Should(Receive())

				close(done)
			}, 5)
		})
	})

	Context("when we fail to maintain our presence", func() {
		BeforeEach(func() {
			maintainStatusChan <- true
			maintainStatusChan <- false
		})

		It("continues to retry", func() {
			Consistently(maintainer.Wait()).ShouldNot(Receive(), "should not shut down")
		})

		It("logs an error message", func() {
			testSink := steno.GetMeTheGlobalTestSink()

			records := []*steno.Record{}

			lockMessageIndex := 0
			Eventually(func() string {
				records = testSink.Records()

				if len(records) > 0 {
					lockMessageIndex := len(records) - 1
					return records[lockMessageIndex].Message
				}

				return ""
			}, 1.0, 0.1).Should(Equal("rep.maintain_presence.lost-lock"))

			Ω(records[lockMessageIndex].Level).Should(Equal(steno.LOG_ERROR))
		})
	})
})
