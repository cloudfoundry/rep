package harvester_test

import (
	"errors"

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
		ports          []models.PortMapping
		processor      harvester.Processor
	)

	itDoesNotClaimTheLRP := func() {
		It("does not process the lrp", func() {
			Ω(bbs.ClaimActualLRPCallCount()).Should(Equal(0))
		})
	}

	itStartsTheLRP := func() {
		It("reports the lrp as running", func() {
			Ω(bbs.StartActualLRPCallCount()).Should(Equal(1))

			actualLrp := bbs.StartActualLRPArgsForCall(0)

			Ω(actualLrp.ProcessGuid).Should(Equal("process-guid"))
			Ω(actualLrp.InstanceGuid).Should(Equal("completed-lrp-guid"))
			Ω(actualLrp.Domain).Should(Equal("my-domain"))
			Ω(actualLrp.Index).Should(Equal(999))
			Ω(actualLrp.Host).Should(Equal(expectedExecutorHost))
			Ω(actualLrp.CellID).Should(Equal(expectedCellId))
		})
	}

	itClaimsTheLRP := func() {
		It("reports the lrp as starting", func() {
			Ω(bbs.ClaimActualLRPCallCount()).Should(Equal(1))

			claimingLRP := bbs.ClaimActualLRPArgsForCall(0)

			Ω(claimingLRP.ProcessGuid).Should(Equal("process-guid"))
			Ω(claimingLRP.InstanceGuid).Should(Equal("completed-lrp-guid"))
			Ω(claimingLRP.CellID).Should(Equal(expectedCellId))
			Ω(claimingLRP.Domain).Should(Equal("my-domain"))
			Ω(claimingLRP.Index).Should(Equal(999))
		})
	}

	itRemovesTheLRP := func() {
		It("removes the lrp", func() {
			Ω(bbs.RemoveActualLRPCallCount()).Should(Equal(1))
			actualLrp := bbs.RemoveActualLRPArgsForCall(0)

			Ω(actualLrp.ProcessGuid).Should(Equal("process-guid"))
			Ω(actualLrp.InstanceGuid).Should(Equal("completed-lrp-guid"))
			Ω(actualLrp.Domain).Should(Equal("my-domain"))
			Ω(actualLrp.Index).Should(Equal(999))
			Ω(actualLrp.Host).Should(Equal(expectedExecutorHost))
			Ω(actualLrp.CellID).Should(Equal(expectedCellId))
			Ω(actualLrp.Ports).Should(Equal(ports))
		})
	}

	itRemovesTheContainer := func() {
		It("removes the container", func() {
			Ω(executorClient.DeleteContainerCallCount()).Should(Equal(1))
			Ω(executorClient.DeleteContainerArgsForCall(0)).Should(Equal("completed-lrp-guid"))
		})
	}

	itDoesNotRemoveTheContainer := func() {
		It("removes the container", func() {
			Ω(executorClient.DeleteContainerCallCount()).Should(Equal(0))
		})
	}

	var container executor.Container

	BeforeEach(func() {
		executorClient = new(fakes.FakeClient)
		bbs = new(fake_bbs.FakeRepBBS)
		tags = executor.Tags{
			rep.LifecycleTag:    rep.LRPLifecycle,
			rep.DomainTag:       "my-domain",
			rep.ProcessGuidTag:  "process-guid",
			rep.ProcessIndexTag: "999",
		}
		ports = []models.PortMapping{
			{ContainerPort: 1234, HostPort: 5678},
		}
		container = executor.Container{
			Guid:  "completed-lrp-guid",
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
