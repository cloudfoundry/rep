package rep_test

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/runtime-schema/models"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Resources", func() {
	Describe("ActualLRPKeyFromContainer", func() {
		var (
			container        executor.Container
			lrpKey           models.ActualLRPKey
			keyConversionErr error
		)

		BeforeEach(func() {
			container = executor.Container{
				Tags: executor.Tags{
					rep.LifecycleTag:    rep.LRPLifecycle,
					rep.DomainTag:       "my-domain",
					rep.ProcessGuidTag:  "process-guid",
					rep.ProcessIndexTag: "999",
				},
				Guid:       "some-instance-guid",
				ExternalIP: "some-external-ip",
				Ports: []executor.PortMapping{
					{
						ContainerPort: 1234,
						HostPort:      6789,
					},
				},
			}
		})

		JustBeforeEach(func() {
			lrpKey, keyConversionErr = rep.ActualLRPKeyFromContainer(container)
		})

		Context("when container is valid", func() {
			It("does not return an error", func() {
				Expect(keyConversionErr).NotTo(HaveOccurred())
			})

			It("converts a valid container without error", func() {
				expectedKey := models.ActualLRPKey{
					ProcessGuid: "process-guid",
					Index:       999,
					Domain:      "my-domain",
				}
				Expect(lrpKey).To(Equal(expectedKey))
			})
		})

		Context("when the container is invalid", func() {
			Context("when the container has no tags", func() {
				BeforeEach(func() {
					container.Tags = nil
				})

				It("reports an error that the tags are missing", func() {
					Expect(keyConversionErr).To(MatchError(rep.ErrContainerMissingTags))
				})
			})

			Context("when the container is missing the process guid tag ", func() {
				BeforeEach(func() {
					delete(container.Tags, rep.ProcessGuidTag)
				})

				It("reports the process_guid is invalid", func() {
					Expect(keyConversionErr).To(HaveOccurred())
					Expect(keyConversionErr.Error()).To(ContainSubstring("process_guid"))
				})
			})

			Context("when the container process index tag is not a number", func() {
				BeforeEach(func() {
					container.Tags[rep.ProcessIndexTag] = "hi there"
				})

				It("reports the index is invalid when constructing ActualLRPKey", func() {
					Expect(keyConversionErr).To(MatchError(rep.ErrInvalidProcessIndex))
				})
			})
		})
	})

	Describe("ActualLRPInstanceKeyFromContainer", func() {

		var (
			container                executor.Container
			lrpInstanceKey           models.ActualLRPInstanceKey
			instanceKeyConversionErr error
			cellID                   string
		)

		BeforeEach(func() {
			container = executor.Container{
				Tags: executor.Tags{
					rep.LifecycleTag:    rep.LRPLifecycle,
					rep.DomainTag:       "my-domain",
					rep.ProcessGuidTag:  "process-guid",
					rep.ProcessIndexTag: "999",
					rep.InstanceGuidTag: "some-instance-guid",
				},
				Guid: "container-guid",
				Ports: []executor.PortMapping{
					{
						ContainerPort: 1234,
						HostPort:      6789,
					},
				},
			}
			cellID = "the-cell-id"
		})

		JustBeforeEach(func() {
			lrpInstanceKey, instanceKeyConversionErr = rep.ActualLRPInstanceKeyFromContainer(container, cellID)
		})

		Context("when the container and cell id are valid", func() {
			It("it does not return an error", func() {
				Expect(instanceKeyConversionErr).NotTo(HaveOccurred())
			})

			It("it creates the correct container key", func() {
				expectedInstanceKey := models.ActualLRPInstanceKey{
					InstanceGuid: "some-instance-guid",
					CellID:       cellID,
				}

				Expect(lrpInstanceKey).To(Equal(expectedInstanceKey))
			})
		})

		Context("when the container is invalid", func() {
			Context("when the container has no tags", func() {
				BeforeEach(func() {
					container.Tags = nil
				})

				It("reports an error that the tags are missing", func() {
					Expect(instanceKeyConversionErr).To(MatchError(rep.ErrContainerMissingTags))
				})
			})

			Context("when the container is missing the instance guid tag ", func() {
				BeforeEach(func() {
					delete(container.Tags, rep.InstanceGuidTag)
				})

				It("returns an invalid instance-guid error", func() {
					Expect(instanceKeyConversionErr.Error()).To(ContainSubstring("instance_guid"))
				})
			})

			Context("when the cell id is invalid", func() {
				BeforeEach(func() {
					cellID = ""
				})

				It("returns an invalid cell id error", func() {
					Expect(instanceKeyConversionErr.Error()).To(ContainSubstring("cell_id"))
				})
			})
		})
	})

	Describe("ActualLRPNetInfoFromContainer", func() {
		var (
			container            executor.Container
			lrpNetInfo           models.ActualLRPNetInfo
			netInfoConversionErr error
		)

		BeforeEach(func() {
			container = executor.Container{
				Tags: executor.Tags{
					rep.LifecycleTag:    rep.LRPLifecycle,
					rep.DomainTag:       "my-domain",
					rep.ProcessGuidTag:  "process-guid",
					rep.ProcessIndexTag: "999",
				},
				Guid:       "some-instance-guid",
				ExternalIP: "some-external-ip",
				Ports: []executor.PortMapping{
					{
						ContainerPort: 1234,
						HostPort:      6789,
					},
				},
			}
		})

		JustBeforeEach(func() {
			lrpNetInfo, netInfoConversionErr = rep.ActualLRPNetInfoFromContainer(container)
		})

		Context("when container and executor host are valid", func() {
			It("does not return an error", func() {
				Expect(netInfoConversionErr).NotTo(HaveOccurred())
			})

			It("returns the correct net info", func() {
				expectedNetInfo := models.ActualLRPNetInfo{
					Ports: []models.PortMapping{
						{
							ContainerPort: 1234,
							HostPort:      6789,
						},
					},
					Address: "some-external-ip",
				}

				Expect(lrpNetInfo).To(Equal(expectedNetInfo))
			})
		})

		Context("when there are no exposed ports", func() {
			BeforeEach(func() {
				container.Ports = nil
			})

			It("does not return an error", func() {
				Expect(netInfoConversionErr).NotTo(HaveOccurred())
			})
		})

		Context("when the executor host is invalid", func() {
			BeforeEach(func() {
				container.ExternalIP = ""
			})

			It("returns an invalid host error", func() {
				Expect(netInfoConversionErr.Error()).To(ContainSubstring("address"))
			})
		})
	})

	Describe("StackPathMap", func() {
		It("deserializes a valid input", func() {
			stackMapPayload := []byte(`{
				"pancakes": "/path/to/lingonberries",
				"waffles": "/where/is/the/syrup"
			}`)

			stackMap, err := rep.UnmarshalStackPathMap(stackMapPayload)
			Expect(err).NotTo(HaveOccurred())

			Expect(stackMap).To(Equal(rep.StackPathMap{
				"waffles":  "/where/is/the/syrup",
				"pancakes": "/path/to/lingonberries",
			}))

		})

		It("errors when passed malformed input", func() {
			_, err := rep.UnmarshalStackPathMap([]byte(`{"foo": ["bar"]}`))
			Expect(err).To(MatchError(ContainSubstring("unmarshal")))
		})
	})
})
