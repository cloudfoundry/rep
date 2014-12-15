package harvester_test

import (
	"errors"
	"strconv"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/harvester"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/bbserrors"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LRP Processor", func() {
	const expectedCellId = "cell-id"
	const expectedExecutorHost = "example.com"

	var (
		executorClient *fakes.FakeClient
		bbs            *fake_bbs.FakeRepBBS
		tags           executor.Tags
		processor      harvester.Processor
		container      executor.Container

		expectedLRPKey          models.ActualLRPKey
		expectedLRPContainerKey models.ActualLRPContainerKey
		expectedNetInfo         models.ActualLRPNetInfo
	)

	itDoesNotClaimTheLRP := func() {
		It("does not process the lrp", func() {
			Ω(bbs.ClaimActualLRPCallCount()).Should(Equal(0))
		})
	}

	itStartsTheLRP := func() {
		It("starts the LRP", func() {
			Ω(bbs.StartActualLRPCallCount()).Should(Equal(1))

			lrpKey, containerKey, netInfo := bbs.StartActualLRPArgsForCall(0)

			Ω(lrpKey).Should(Equal(expectedLRPKey))
			Ω(containerKey).Should(Equal(expectedLRPContainerKey))
			Ω(netInfo).Should(Equal(expectedNetInfo))
		})
	}

	itClaimsTheLRP := func() {
		It("claims the LRP", func() {
			Ω(bbs.ClaimActualLRPCallCount()).Should(Equal(1))

			lrpKey, containerKey := bbs.ClaimActualLRPArgsForCall(0)

			Ω(lrpKey).Should(Equal(expectedLRPKey))
			Ω(containerKey).Should(Equal(expectedLRPContainerKey))
		})
	}

	itRemovesTheLRP := func() {
		It("removes the LRP", func() {
			Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(1))
			lrpKey, containerKey := bbs.RemoveActualLRPArgsForCall(0)

			Ω(lrpKey).Should(Equal(expectedLRPKey))
			Ω(containerKey).Should(Equal(expectedLRPContainerKey))
		})
	}

	itRemovesTheContainer := func() {
		It("removes the container", func() {
			Ω(executorClient.DeleteContainerCallCount()).Should(Equal(1))
			Ω(executorClient.DeleteContainerArgsForCall(0)).Should(Equal(expectedLRPContainerKey.InstanceGuid))
		})
	}

	itDoesNotRemoveTheContainer := func() {
		It("removes the container", func() {
			Ω(executorClient.DeleteContainerCallCount()).Should(Equal(0))
		})
	}

	BeforeEach(func() {
		expectedLRPKey = models.NewActualLRPKey("process-guid", 999, "my-domain")
		expectedLRPContainerKey = models.NewActualLRPContainerKey("instance-guid", expectedCellId)
		expectedNetInfo = models.NewActualLRPNetInfo(expectedExecutorHost, []models.PortMapping{{ContainerPort: 1234, HostPort: 5678}})

		executorClient = new(fakes.FakeClient)
		bbs = new(fake_bbs.FakeRepBBS)
		tags = executor.Tags{
			rep.LifecycleTag:    rep.LRPLifecycle,
			rep.ProcessGuidTag:  expectedLRPKey.ProcessGuid,
			rep.ProcessIndexTag: strconv.Itoa(expectedLRPKey.Index),
			rep.DomainTag:       expectedLRPKey.Domain,
		}
		container = executor.Container{
			Guid:  expectedLRPContainerKey.InstanceGuid,
			State: executor.StateCompleted,
			Tags:  tags,
			Ports: []executor.PortMapping{
				{ContainerPort: 1234, HostPort: 5678},
			},
		}
		processor = harvester.NewLRPProcessor(
			expectedCellId,
			expectedExecutorHost,
			lagertest.NewTestLogger("test"),
			bbs,
			executorClient,
		)
	})

	JustBeforeEach(func() {
		processor.Process(container)
	})

	Context("when the container state is reserved", func() {
		BeforeEach(func() {
			container.State = executor.StateReserved
		})

		itDoesNotClaimTheLRP()
	})

	Context("when the container state is initializing", func() {
		BeforeEach(func() {
			container.State = executor.StateInitializing
		})

		itClaimsTheLRP()

		Context("but the BBS reports the LRP cannot be claimed", func() {
			BeforeEach(func() {
				bbs.ClaimActualLRPReturns(nil, bbserrors.ErrActualLRPCannotBeClaimed)
			})

			itRemovesTheContainer()
		})

		Context("but the BBS reports some other error", func() {
			BeforeEach(func() {
				bbs.ClaimActualLRPReturns(nil, errors.New("oh no!"))
			})

			itDoesNotRemoveTheContainer()
		})
	})

	Context("when the container state is created", func() {
		BeforeEach(func() {
			container.State = executor.StateCreated
		})

		itClaimsTheLRP()

		Context("but the BBS reports the LRP cannot be claimed", func() {
			BeforeEach(func() {
				bbs.ClaimActualLRPReturns(nil, bbserrors.ErrActualLRPCannotBeClaimed)
			})

			itRemovesTheContainer()
		})

		Context("but the BBS reports some other error", func() {
			BeforeEach(func() {
				bbs.ClaimActualLRPReturns(nil, errors.New("oh no!"))
			})

			itDoesNotRemoveTheContainer()
		})
	})

	Context("when the container state is running", func() {
		BeforeEach(func() {
			container.State = executor.StateRunning
		})

		itStartsTheLRP()

		Context("but the BBS reports the LRP cannot be started", func() {
			BeforeEach(func() {
				bbs.StartActualLRPReturns(nil, bbserrors.ErrActualLRPCannotBeStarted)
			})

			itRemovesTheContainer()
		})

		Context("but the BBS reports some other error", func() {
			BeforeEach(func() {
				bbs.ClaimActualLRPReturns(nil, errors.New("oh no!"))
			})

			itDoesNotRemoveTheContainer()
		})
	})

	Context("when the container state is completed", func() {
		BeforeEach(func() {
			container.State = executor.StateCompleted
		})

		itRemovesTheLRP()
		itRemovesTheContainer()
	})

	Context("when the container is invalid", func() {
		BeforeEach(func() {
			container.Tags = nil
		})

		itDoesNotClaimTheLRP()
	})
})
