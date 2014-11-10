package reaper_test

import (
	"errors"
	"os"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	efakes "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep/reaper"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/pivotal-golang/timer/fake_timer"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Task Reaper", func() {
	var (
		executorClient *efakes.FakeClient

		pollInterval    time.Duration
		timer           *fake_timer.FakeTimer
		actualLRPReaper ifrit.Runner
		process         ifrit.Process
		bbs             *fake_bbs.FakeRepBBS
	)

	BeforeEach(func() {
		pollInterval = 100 * time.Millisecond
		timer = fake_timer.NewFakeTimer(time.Now())
		executorClient = new(efakes.FakeClient)

		bbs = new(fake_bbs.FakeRepBBS)
		actualLRPReaper = reaper.NewActualLRPReaper(pollInterval, timer, "executor-id", bbs, executorClient, lagertest.NewTestLogger("test"))
	})

	JustBeforeEach(func() {
		process = ifrit.Invoke(actualLRPReaper)
	})

	AfterEach(func() {
		process.Signal(os.Interrupt)
		Eventually(process.Wait()).Should(Receive())
	})

	Context("when the timer elapses", func() {
		JustBeforeEach(func() {
			timer.Elapse(pollInterval)
		})

		It("gets actual LRPs for this executor from the BBS", func() {
			Eventually(bbs.GetAllActualLRPsByExecutorIDCallCount).Should(Equal(1))
			Ω(bbs.GetAllActualLRPsByExecutorIDArgsForCall(0)).Should(Equal("executor-id"))
		})

		Context("when there are actual LRPs for this executor in the BBS", func() {
			BeforeEach(func() {
				bbs.GetAllActualLRPsByExecutorIDReturns([]models.ActualLRP{
					models.ActualLRP{
						InstanceGuid: "instance-guid-1",
					},
					models.ActualLRP{
						InstanceGuid: "instance-guid-2",
					},
				}, nil)
			})

			Context("but the executor doesn't know about these actual LRPs", func() {
				BeforeEach(func() {
					executorClient.GetContainerReturns(executor.Container{}, executor.ErrContainerNotFound)
				})

				It("remove those actual LRPs from the BBS", func() {
					Eventually(bbs.RemoveActualLRPCallCount).Should(Equal(2))

					actualLRP1 := bbs.RemoveActualLRPArgsForCall(0)
					Ω(actualLRP1.InstanceGuid).Should(Equal("instance-guid-1"))

					actualLRP2 := bbs.RemoveActualLRPArgsForCall(1)
					Ω(actualLRP2.InstanceGuid).Should(Equal("instance-guid-2"))
				})
			})

			Context("when GetContainer fails for some other reason", func() {
				BeforeEach(func() {
					executorClient.GetContainerReturns(executor.Container{}, errors.New("executor error"))
				})

				It("does not mark those tasks as complete", func() {
					Consistently(bbs.RemoveActualLRPCallCount).Should(Equal(0))
				})
			})

			Context("and the executor does know about these actual LRPs", func() {
				BeforeEach(func() {
					executorClient.GetContainerReturns(executor.Container{}, nil)
				})

				It("does not mark those tasks as complete", func() {
					Consistently(bbs.RemoveActualLRPCallCount).Should(Equal(0))
				})
			})
		})

		Context("when getting actual LRPs from the BBS fails", func() {
			BeforeEach(func() {
				bbs.GetAllActualLRPsByExecutorIDReturns(nil, errors.New("bbs error"))
			})

			It("does not die", func() {
				Consistently(process.Wait()).ShouldNot(Receive())
			})

			Context("and the timer elapses again", func() {
				JustBeforeEach(func() {
					timer.Elapse(pollInterval)
				})

				It("happily continues on to next time", func() {
					Eventually(bbs.GetAllActualLRPsByExecutorIDCallCount).Should(Equal(2))
				})
			})
		})
	})
})
