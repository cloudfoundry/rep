package services_bbs_test

import (
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/storeadapter"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/cloudfoundry-incubator/runtime-schema/bbs/services_bbs"
)

var _ = Context("Getting Generic Services", func() {
	var bbs *ServicesBBS

	BeforeEach(func() {
		bbs = New(etcdClient, lagertest.NewTestLogger("test"))
	})

	Describe("GetServiceRegistrations", func() {
		var registrations models.ServiceRegistrations
		var registrationsErr error

		JustBeforeEach(func() {
			registrations, registrationsErr = bbs.GetServiceRegistrations()
		})

		Context("when etcd returns sucessfully", func() {
			BeforeEach(func() {
				serviceNodes := []storeadapter.StoreNode{
					{
						Key:   "/v1/executor/guid-0",
						Value: []byte("{}"),
					},
					{
						Key:   "/v1/executor/guid-1",
						Value: []byte("{}"),
					},
					{
						Key:   "/v1/file_server/guid-0",
						Value: []byte("http://example.com/file-server"),
					},
				}
				err := etcdClient.SetMulti(serviceNodes)
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("does not return an error", func() {
				Ω(registrationsErr).ShouldNot(HaveOccurred())
			})

			It("returns the executor service registrations", func() {
				executorRegistrations := registrations.FilterByName(models.ExecutorServiceName)
				Ω(executorRegistrations).Should(HaveLen(2))
				Ω(executorRegistrations).Should(ContainElement(models.ServiceRegistration{
					Name: models.ExecutorServiceName, Id: "guid-0",
				}))
				Ω(executorRegistrations).Should(ContainElement(models.ServiceRegistration{
					Name: models.ExecutorServiceName, Id: "guid-1",
				}))
			})

			It("returns the file-server service registrations", func() {
				Ω(registrations.FilterByName(models.FileServerServiceName)).Should(Equal(models.ServiceRegistrations{
					{Name: models.FileServerServiceName, Id: "guid-0"},
				}))
			})
		})

		Context("when etcd comes up empty", func() {
			It("does not return an error", func() {
				Ω(registrationsErr).ShouldNot(HaveOccurred())
			})

			It("returns empty registrations", func() {
				Ω(registrations).Should(BeEmpty())
			})
		})
	})
})
