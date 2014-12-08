package rep_test

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/runtime-schema/models"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Resources", func() {
	Describe("ActualLRPFromContainer", func() {
		const cellID = "the-cell-id"
		const executorHost = "executor.example.com:9753"

		var container executor.Container
		var actualLRP *models.ActualLRP
		var conversionErr error

		BeforeEach(func() {
			container = executor.Container{
				Tags: executor.Tags{
					rep.LifecycleTag:    rep.LRPLifecycle,
					rep.DomainTag:       "my-domain",
					rep.ProcessGuidTag:  "process-guid",
					rep.ProcessIndexTag: "999",
				},
				Guid: "some-instance-guid",
				Ports: []executor.PortMapping{
					{
						ContainerPort: 1234,
						HostPort:      6789,
					},
				},
			}
		})

		JustBeforeEach(func() {
			actualLRP, conversionErr = rep.ActualLRPFromContainer(container, cellID, executorHost)
		})

		It("converts a valid container without error", func() {
			Ω(conversionErr).ShouldNot(HaveOccurred())

			expectedLRP := models.ActualLRP{
				ProcessGuid:  "process-guid",
				Index:        999,
				Domain:       "my-domain",
				InstanceGuid: "some-instance-guid",
				CellID:       cellID,
				State:        models.ActualLRPStateInvalid,

				Ports: []models.PortMapping{
					{
						ContainerPort: 1234,
						HostPort:      6789,
					},
				},
				Host: executorHost,
			}

			Ω(actualLRP).Should(Equal(&expectedLRP))
		})

		Context("when the container is invalid", func() {
			Context("when the container has no tags", func() {
				BeforeEach(func() {
					container.Tags = nil
				})

				It("reports an error that the tags are missing", func() {
					Ω(conversionErr).Should(MatchError(rep.ErrContainerMissingTags))
				})
			})

			Context("when the container is missing the process guid tag", func() {
				BeforeEach(func() {
					delete(container.Tags, rep.ProcessGuidTag)
				})

				It("reports the process_guid is invalid", func() {
					Ω(conversionErr).Should(HaveOccurred())
					Ω(conversionErr.Error()).Should(ContainSubstring("process_guid"))
				})
			})

			Context("when the container process index tag is not a number", func() {
				BeforeEach(func() {
					container.Tags[rep.ProcessIndexTag] = "hi there"
				})

				It("reports the index is invalid", func() {
					Ω(conversionErr).Should(MatchError(rep.ErrInvalidProcessIndex))
				})
			})
		})
	})
})
