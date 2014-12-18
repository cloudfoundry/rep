package reaper_test

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep/gatherer/fake_gatherer"
	"github.com/cloudfoundry-incubator/rep/reaper"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Actual LRP Reaper", func() {
	var (
		actualLRPReaper *reaper.ActualLRPReaper
		bbs             *fake_bbs.FakeRepBBS
		snapshot        *fake_gatherer.FakeSnapshot
	)

	BeforeEach(func() {
		bbs = new(fake_bbs.FakeRepBBS)
		snapshot = new(fake_gatherer.FakeSnapshot)
	})

	JustBeforeEach(func() {
		actualLRPReaper = reaper.NewActualLRPReaper(bbs, lagertest.NewTestLogger("test"))
		actualLRPReaper.Process(snapshot)
	})

	It("gets actual LRPs for this executor from the BBS", func() {
		Ω(snapshot.ActualLRPsCallCount()).Should(Equal(1))
	})

	Context("when there are actual LRPs for this executor in the BBS", func() {
		var expectedLRPKey models.ActualLRPKey
		var expectedContainerKey models.ActualLRPContainerKey

		BeforeEach(func() {
			expectedLRPKey = models.NewActualLRPKey("p-guid", 1, "d")
			expectedContainerKey = models.NewActualLRPContainerKey("i-guid", "c-guid")
			snapshot.ActualLRPsReturns([]models.ActualLRP{
				models.ActualLRP{
					ActualLRPKey:          expectedLRPKey,
					ActualLRPContainerKey: expectedContainerKey,
				},
			})
		})

		Context("but the executor doesn't know about these actual LRPs", func() {
			BeforeEach(func() {
				snapshot.GetContainerReturns(nil, false)
			})

			It("remove those actual LRPs from the BBS", func() {
				Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(1))

				lrpKey, containerKey, _ := bbs.RemoveActualLRPArgsForCall(0)
				Ω(lrpKey).Should(Equal(expectedLRPKey))
				Ω(containerKey).Should(Equal(expectedContainerKey))

			})
		})

		Context("and the executor has a container for the actual LRP", func() {
			BeforeEach(func() {
				snapshot.GetContainerReturns(&executor.Container{}, true)
			})

			It("does not mark those tasks as complete", func() {
				Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(0))
			})
		})
	})
})
