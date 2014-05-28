package maintain_test

import (
	"os"
	"syscall"
	"time"

	"github.com/cloudfoundry-incubator/rep/maintain"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	steno "github.com/cloudfoundry/gosteno"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Maintain Presence", func() {
	var repPresence models.RepPresence
	var heartbeatInterval = 1 * time.Second

	var fakeBBS *fake_bbs.FakeRepBBS
	var sigChan chan os.Signal
	var logger *steno.Logger

	var maintainer ifrit.Process

	BeforeSuite(func() {
		steno.EnterTestMode(steno.LOG_DEBUG)
	})

	BeforeEach(func() {
		repPresence = models.RepPresence{
			RepID: "rep-id",
			Stack: "lucid64",
		}
		fakeBBS = fake_bbs.NewFakeRepBBS()
		logger = steno.NewLogger("test-logger")
		sigChan = make(chan os.Signal, 1)
	})

	JustBeforeEach(func() {
		maintainer = ifrit.Envoke(maintain.New(repPresence, fakeBBS, logger, heartbeatInterval))
	})

	AfterEach(func() {
		maintainer.Signal(syscall.SIGTERM)
		<-maintainer.Wait()
	})

	Context("when maintaining presence", func() {
		It("should maintain presence", func() {
			Eventually(fakeBBS.GetMaintainRepPresence).Should(Equal(repPresence))
		})
	})

	Context("when we fail to maintain our presence", func() {
		BeforeEach(func() {
			fakeBBS.MaintainRepPresenceOutput.Presence = &fake_bbs.FakePresence{
				MaintainStatus: false,
			}
		})

		It("continues to retry", func() {
			Consistently(maintainer.Wait()).ShouldNot(Receive())
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
