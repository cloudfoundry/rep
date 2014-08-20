package services_bbs_test

import (
	"os"
	"time"

	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/cloudfoundry-incubator/runtime-schema/bbs/services_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models/factories"
)

var _ = Describe("Fetching available file servers", func() {
	var (
		bbs      *ServicesBBS
		interval time.Duration
	)

	BeforeEach(func() {
		interval = 1 * time.Second
		bbs = New(etcdClient, lagertest.NewTestLogger("test"))
	})

	Describe("MaintainFileServerPresence", func() {
		var fileServerURL string
		var fileServerId string
		var heartbeat ifrit.Process

		BeforeEach(func() {
			fileServerURL = "stubFileServerURL"
			fileServerId = factories.GenerateGuid()

			heartbeat = ifrit.Envoke(bbs.NewFileServerHeartbeat(fileServerURL, fileServerId, interval))
		})

		AfterEach(func() {
			heartbeat.Signal(os.Interrupt)
			Eventually(heartbeat.Wait()).Should(Receive(BeNil()))
		})

		It("should put /file_server/FILE_SERVER_ID in the store with a TTL", func() {
			node, err := etcdClient.Get("/v1/file_server/" + fileServerId)
			Ω(err).ShouldNot(HaveOccurred())
			Ω(node.TTL).ShouldNot(BeZero())
			Ω(node.Value).Should(Equal([]byte(fileServerURL)))
		})
	})

	Describe("Getting file servers", func() {
		var fileServerURL string
		var fileServerId string
		var heartbeat ifrit.Process

		Context("when there are available file servers", func() {
			BeforeEach(func() {
				fileServerURL = "http://128.70.3.29:8012"
				fileServerId = factories.GenerateGuid()

				heartbeat = ifrit.Envoke(bbs.NewFileServerHeartbeat(fileServerURL, fileServerId, interval))
			})

			AfterEach(func() {
				heartbeat.Signal(os.Interrupt)
				Eventually(heartbeat.Wait()).Should(Receive(BeNil()))
			})

			Describe("GetAvailableFileServer", func() {
				It("should get from /v1/file_server/", func() {
					url, err := bbs.GetAvailableFileServer()
					Ω(err).ShouldNot(HaveOccurred())
					Ω(url).Should(Equal(fileServerURL))
				})
			})

			Describe("GetAllFileServers", func() {
				It("should return all file server urls", func() {
					urls, err := bbs.GetAllFileServers()
					Ω(err).ShouldNot(HaveOccurred())
					Ω(urls).Should(Equal([]string{fileServerURL}))
				})
			})
		})

		Context("when there are several available file servers", func() {
			var otherFileServerURL string
			var otherHeartbeat ifrit.Process

			BeforeEach(func() {
				fileServerURL = "http://guy"
				fileServerId = factories.GenerateGuid()

				otherFileServerURL = "http://other.guy"
				otherFileServerId := factories.GenerateGuid()

				heartbeat = ifrit.Envoke(bbs.NewFileServerHeartbeat(fileServerURL, fileServerId, interval))
				otherHeartbeat = ifrit.Envoke(bbs.NewFileServerHeartbeat(otherFileServerURL, otherFileServerId, interval))
			})

			AfterEach(func() {
				heartbeat.Signal(os.Interrupt)
				Eventually(heartbeat.Wait()).Should(Receive(BeNil()))
				otherHeartbeat.Signal(os.Interrupt)
				Eventually(otherHeartbeat.Wait()).Should(Receive(BeNil()))
			})

			Describe("GetAvailableFileServer", func() {
				It("should pick one at random", func() {
					result := map[string]bool{}

					for i := 0; i < 10; i++ {
						url, err := bbs.GetAvailableFileServer()
						Ω(err).ShouldNot(HaveOccurred())
						result[url] = true
					}

					Ω(result).Should(HaveLen(2))
					Ω(result).Should(HaveKey(fileServerURL))
					Ω(result).Should(HaveKey(otherFileServerURL))
				})
			})

			Describe("GetAllFileServers", func() {
				It("should return all file server urls", func() {
					urls, err := bbs.GetAllFileServers()
					Ω(err).ShouldNot(HaveOccurred())
					Ω(urls).Should(ConsistOf([]string{fileServerURL, otherFileServerURL}))
				})
			})
		})

		Context("when there are none", func() {
			Describe("GetAvailableFileServer", func() {
				It("should error", func() {
					url, err := bbs.GetAvailableFileServer()
					Ω(err).Should(HaveOccurred())
					Ω(url).Should(BeZero())
				})
			})

			Describe("GetAllFileServers", func() {
				It("should return empty", func() {
					urls, err := bbs.GetAllFileServers()
					Ω(err).ShouldNot(HaveOccurred())
					Ω(urls).Should(BeEmpty())
				})
			})
		})
	})
})
