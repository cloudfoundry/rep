package harvester_test

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/harvester"
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
	})

	Context("when the container state is created", func() {
		BeforeEach(func() {
			container.State = executor.StateCreated
		})

		itClaimsTheLRP()
	})

	Context("when the container state is running", func() {
		BeforeEach(func() {
			container.State = executor.StateRunning
		})

		itStartsTheLRP()
	})

	Context("when the container state is completed", func() {
		BeforeEach(func() {
			container.State = executor.StateCompleted
		})

		itRemovesTheLRP()
		itRemovesTheContainer()
	})

	Context("when the container is invalid", func() {
		Context("when the container is missing a guid", func() {
			BeforeEach(func() {
				container.Guid = ""
			})

			itDoesNotClaimTheLRP()
		})

		Context("when the container has no tags", func() {
			BeforeEach(func() {
				container.Tags = nil
			})

			itDoesNotClaimTheLRP()
		})

		Context("when the container is missing the process guid tag", func() {
			BeforeEach(func() {
				delete(container.Tags, rep.ProcessGuidTag)
			})

			itDoesNotClaimTheLRP()
		})

		Context("when the container is missing the domain tag", func() {
			BeforeEach(func() {
				delete(container.Tags, rep.DomainTag)
			})

			itDoesNotClaimTheLRP()
		})

		Context("when the container is missing the process index tag", func() {
			BeforeEach(func() {
				delete(container.Tags, rep.ProcessIndexTag)
			})

			itDoesNotClaimTheLRP()
		})

		Context("when the container process index tag is not a number", func() {
			BeforeEach(func() {
				container.Tags[rep.ProcessIndexTag] = "hi there"
			})

			itDoesNotClaimTheLRP()
		})
	})
})
