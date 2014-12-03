package gatherer_test

import (
	"errors"
	"os"
	"time"

	efakes "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep/gatherer"
	"github.com/cloudfoundry-incubator/rep/gatherer/fake_gatherer"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry/gunk/timeprovider/faketimeprovider"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"
)

var _ = Describe("Gatherer", func() {
	var (
		executorClient *efakes.FakeClient
		bbs            *fake_bbs.FakeRepBBS
		processors     []*fake_gatherer.FakeProcessor

		pollInterval time.Duration
		timeProvider *faketimeprovider.FakeTimeProvider
		runner       ifrit.Runner
		process      ifrit.Process
	)

	BeforeEach(func() {
		pollInterval = 100 * time.Millisecond
		timeProvider = faketimeprovider.New(time.Now())
		executorClient = new(efakes.FakeClient)

		bbs = new(fake_bbs.FakeRepBBS)
		fp1 := &fake_gatherer.FakeProcessor{}
		fp2 := &fake_gatherer.FakeProcessor{}
		processors = []*fake_gatherer.FakeProcessor{fp1, fp2}
		runner = gatherer.NewGatherer(
			pollInterval,
			timeProvider,
			[]gatherer.Processor{fp1, fp2},
			"cell-id",
			bbs,
			executorClient,
			lagertest.NewTestLogger("test"),
		)
	})

	JustBeforeEach(func() {
		process = ifrit.Invoke(runner)
	})

	AfterEach(func() {
		process.Signal(os.Interrupt)
		Eventually(process.Wait()).Should(Receive())
	})

	Context("when the timer elapses", func() {
		JustBeforeEach(func() {
			timeProvider.Increment(pollInterval)
		})

		It("invokes all the processors", func() {
			for _, p := range processors {
				Eventually(p.ProcessCallCount).Should(Equal(1))
				Ω(p.ProcessArgsForCall(0)).ShouldNot(BeNil())
			}
		})

		Context("when an error occurs during snapshot", func() {
			BeforeEach(func() {
				bbs.ActualLRPsByCellIDReturns(nil, errors.New("bbs error"))
			})

			It("does not invoke the processors", func() {
				for _, p := range processors {
					Consistently(p.ProcessCallCount).Should(Equal(0))
				}
			})
		})
	})
})
