package evacuation_test

import (
	"errors"
	"os"

	"github.com/cloudfoundry-incubator/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/bbs/models/test/model_helpers"
	"github.com/cloudfoundry-incubator/rep/evacuation"
	fake_metrics_sender "github.com/cloudfoundry/dropsonde/metric_sender/fake"
	"github.com/cloudfoundry/dropsonde/metrics"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"
)

var _ = Describe("EvacuationCleanup", func() {
	var (
		logger *lagertest.TestLogger
		cellID string

		fakeBBSClient     *fake_bbs.FakeClient
		fakeMetricsSender *fake_metrics_sender.FakeMetricSender

		cleanup        *evacuation.EvacuationCleanup
		cleanupProcess ifrit.Process

		errCh  chan error
		doneCh chan struct{}
	)

	BeforeEach(func() {
		cellID = "the-cell-id"
		logger = lagertest.NewTestLogger("cleanup")

		fakeBBSClient = &fake_bbs.FakeClient{}
		fakeMetricsSender = fake_metrics_sender.NewFakeMetricSender()
		metrics.Initialize(fakeMetricsSender, nil)

		errCh = make(chan error, 1)
		doneCh = make(chan struct{})
		cleanup = evacuation.NewEvacuationCleanup(logger, cellID, fakeBBSClient)
	})

	JustBeforeEach(func() {
		cleanupProcess = ginkgomon.Invoke(cleanup)
		go func() {
			err := <-cleanupProcess.Wait()
			errCh <- err
			close(doneCh)
		}()
	})

	AfterEach(func() {
		cleanupProcess.Signal(os.Interrupt)
		Eventually(doneCh).Should(BeClosed())
	})

	It("does not exit", func() {
		Consistently(errCh).ShouldNot(Receive())
	})

	Context("when the process is signalled", func() {
		var (
			actualLRPGroups                          []*models.ActualLRPGroup
			actualLRPGroup, evacuatingActualLRPGroup *models.ActualLRPGroup
		)

		BeforeEach(func() {
			runningActualLRPGroup := &models.ActualLRPGroup{
				Instance: model_helpers.NewValidActualLRP("running-process-guid", 0),
			}
			evacuatingActualLRPGroup = &models.ActualLRPGroup{
				Evacuating: model_helpers.NewValidActualLRP("evacuating-process-guid", 0),
			}
			actualLRPGroup = &models.ActualLRPGroup{
				Instance:   model_helpers.NewValidActualLRP("process-guid", 0),
				Evacuating: model_helpers.NewValidActualLRP("process-guid", 0),
			}

			actualLRPGroups = []*models.ActualLRPGroup{
				runningActualLRPGroup,
				evacuatingActualLRPGroup,
				actualLRPGroup,
			}

			fakeBBSClient.ActualLRPGroupsReturns(actualLRPGroups, nil)
		})

		JustBeforeEach(func() {
			cleanupProcess.Signal(os.Kill)
		})

		It("removes all evacuating actual lrps associated with the cell", func() {
			Eventually(errCh).Should(Receive(nil))
			Expect(fakeBBSClient.ActualLRPGroupsCallCount()).To(Equal(1))
			filter := fakeBBSClient.ActualLRPGroupsArgsForCall(0)
			Expect(filter).To(Equal(models.ActualLRPFilter{CellID: cellID}))

			Expect(fakeBBSClient.RemoveEvacuatingActualLRPCallCount()).To(Equal(2))

			lrpKey, lrpInstanceKey := fakeBBSClient.RemoveEvacuatingActualLRPArgsForCall(0)
			Expect(*lrpKey).To(Equal(evacuatingActualLRPGroup.Evacuating.ActualLRPKey))
			Expect(*lrpInstanceKey).To(Equal(evacuatingActualLRPGroup.Evacuating.ActualLRPInstanceKey))

			lrpKey, lrpInstanceKey = fakeBBSClient.RemoveEvacuatingActualLRPArgsForCall(1)
			Expect(*lrpKey).To(Equal(actualLRPGroup.Evacuating.ActualLRPKey))
			Expect(*lrpInstanceKey).To(Equal(actualLRPGroup.Evacuating.ActualLRPInstanceKey))
		})

		It("emits a metric for the number of stranded evacuating actual lrps", func() {
			Eventually(errCh).Should(Receive(nil))
			Expect(fakeMetricsSender.GetValue("StrandedEvacuatingActualLRPs").Value).To(BeEquivalentTo(2))
		})

		Describe("when fetching the actual lrp groups fails", func() {
			BeforeEach(func() {
				fakeBBSClient.ActualLRPGroupsReturns(nil, errors.New("failed"))
			})

			It("exits with an error", func() {
				var err error
				Eventually(errCh).Should(Receive(&err))
				Expect(err).To(Equal(errors.New("failed")))
			})
		})

		Describe("when removing the evacuating actual lrp fails", func() {
			BeforeEach(func() {
				fakeBBSClient.RemoveEvacuatingActualLRPReturns(errors.New("failed"))
			})

			It("continues removing evacuating actual lrps", func() {
				Eventually(errCh).Should(Receive(nil))
				Expect(fakeBBSClient.RemoveEvacuatingActualLRPCallCount()).To(Equal(2))
			})
		})
	})
})
