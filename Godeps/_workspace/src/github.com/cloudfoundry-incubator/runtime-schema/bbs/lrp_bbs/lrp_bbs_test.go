package lrp_bbs_test

import (
	. "github.com/cloudfoundry-incubator/runtime-schema/bbs/lrp_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/storeadapter"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LRP", func() {
	var bbs *LongRunningProcessBBS

	BeforeEach(func() {
		bbs = New(etcdClient)
	})

	Describe("DesireLongRunningProcess", func() {
		var lrp models.DesiredLRP

		BeforeEach(func() {
			lrp = models.DesiredLRP{
				ProcessGuid: "some-process-guid",
				Instances:   5,
				Stack:       "some-stack",
				MemoryMB:    1024,
				DiskMB:      512,
				Routes:      []string{"route-1", "route-2"},
			}
		})

		It("creates /v1/desired/<process-guid>/<index>", func() {
			err := bbs.DesireLongRunningProcess(lrp)
			Ω(err).ShouldNot(HaveOccurred())

			node, err := etcdClient.Get("/v1/desired/some-process-guid")
			Ω(err).ShouldNot(HaveOccurred())
			Ω(node.Value).Should(Equal(lrp.ToJSON()))
		})

		Context("when the store is out of commission", func() {
			itRetriesUntilStoreComesBack(func() error {
				return bbs.DesireLongRunningProcess(lrp)
			})
		})
	})

	Describe("Adding and removing actual LRPs", func() {
		var lrp models.LRP

		BeforeEach(func() {
			lrp = models.LRP{
				ProcessGuid:  "some-process-guid",
				InstanceGuid: "some-instance-guid",
				Index:        1,

				Host: "1.2.3.4",
				Ports: []models.PortMapping{
					{ContainerPort: 8080, HostPort: 65100},
					{ContainerPort: 8081, HostPort: 65101},
				},
			}
		})

		Describe("ReportActualLongRunningProcessAsStarting", func() {
			It("creates /v1/actual/<process-guid>/<index>/<instance-guid>", func() {
				err := bbs.ReportActualLongRunningProcessAsStarting(lrp)
				Ω(err).ShouldNot(HaveOccurred())

				node, err := etcdClient.Get("/v1/actual/some-process-guid/1/some-instance-guid")
				Ω(err).ShouldNot(HaveOccurred())

				expectedLRP := lrp
				expectedLRP.State = models.LRPStateStarting
				Ω(node.Value).Should(MatchJSON(expectedLRP.ToJSON()))
			})

			Context("when the store is out of commission", func() {
				itRetriesUntilStoreComesBack(func() error {
					return bbs.ReportActualLongRunningProcessAsStarting(lrp)
				})
			})
		})

		Describe("ReportActualLongRunningProcessAsRunning", func() {
			It("creates /v1/actual/<process-guid>/<index>/<instance-guid>", func() {
				err := bbs.ReportActualLongRunningProcessAsRunning(lrp)
				Ω(err).ShouldNot(HaveOccurred())

				node, err := etcdClient.Get("/v1/actual/some-process-guid/1/some-instance-guid")
				Ω(err).ShouldNot(HaveOccurred())

				expectedLRP := lrp
				expectedLRP.State = models.LRPStateRunning
				Ω(node.Value).Should(MatchJSON(expectedLRP.ToJSON()))
			})

			Context("when the store is out of commission", func() {
				itRetriesUntilStoreComesBack(func() error {
					return bbs.ReportActualLongRunningProcessAsRunning(lrp)
				})
			})
		})

		Describe("RemoveActualLongRunningProcess", func() {
			BeforeEach(func() {
				bbs.ReportActualLongRunningProcessAsStarting(lrp)
			})

			It("should remove the LRP", func() {
				err := bbs.RemoveActualLongRunningProcess(lrp)
				Ω(err).ShouldNot(HaveOccurred())

				_, err = etcdClient.Get("/v1/actual/some-process-guid/1/some-instance-guid")
				Ω(err).Should(MatchError(storeadapter.ErrorKeyNotFound))
			})

			Context("when the store is out of commission", func() {
				itRetriesUntilStoreComesBack(func() error {
					return bbs.RemoveActualLongRunningProcess(lrp)
				})
			})
		})
	})

	Describe("WatchForDesiredLRPChanges", func() {
		var (
			events  <-chan models.DesiredLRPChange
			stop    chan<- bool
			errors  <-chan error
			stopped bool
		)

		lrp := models.DesiredLRP{
			ProcessGuid: "some-process-guid",
			Instances:   5,
			Stack:       "some-stack",
			MemoryMB:    1024,
			DiskMB:      512,
			Routes:      []string{"route-1", "route-2"},
		}

		BeforeEach(func() {
			events, stop, errors = bbs.WatchForDesiredLRPChanges()
		})

		AfterEach(func() {
			if !stopped {
				stop <- true
			}
		})

		It("sends an event down the pipe for creates", func() {
			err := bbs.DesireLongRunningProcess(lrp)
			Ω(err).ShouldNot(HaveOccurred())

			Eventually(events).Should(Receive(Equal(models.DesiredLRPChange{
				Before: nil,
				After:  &lrp,
			})))
		})

		It("sends an event down the pipe for updates", func() {
			err := bbs.DesireLongRunningProcess(lrp)
			Ω(err).ShouldNot(HaveOccurred())

			Eventually(events).Should(Receive())

			changedLRP := lrp
			changedLRP.Instances++

			err = bbs.DesireLongRunningProcess(changedLRP)
			Ω(err).ShouldNot(HaveOccurred())

			Eventually(events).Should(Receive(Equal(models.DesiredLRPChange{
				Before: &lrp,
				After:  &changedLRP,
			})))
		})

		It("sends an event down the pipe for deletes", func() {
			err := bbs.DesireLongRunningProcess(lrp)
			Ω(err).ShouldNot(HaveOccurred())

			Eventually(events).Should(Receive())

			err = etcdClient.Delete(shared.DesiredLRPSchemaPath(lrp))
			Ω(err).ShouldNot(HaveOccurred())

			Eventually(events).Should(Receive(Equal(models.DesiredLRPChange{
				Before: &lrp,
				After:  nil,
			})))
		})

		It("closes the events and errors channel when told to stop", func() {
			stop <- true
			stopped = true

			err := bbs.DesireLongRunningProcess(lrp)
			Ω(err).ShouldNot(HaveOccurred())

			Ω(events).Should(BeClosed())
			Ω(errors).Should(BeClosed())
		})
	})

	Describe("WatchForActualLongRunningProcesses", func() {
		var (
			events  <-chan models.LRP
			stop    chan<- bool
			errors  <-chan error
			stopped bool
		)

		lrp := models.LRP{ProcessGuid: "some-process-guid", State: models.LRPStateRunning}

		BeforeEach(func() {
			events, stop, errors = bbs.WatchForActualLongRunningProcesses()
		})

		AfterEach(func() {
			if !stopped {
				stop <- true
			}
		})

		It("sends an event down the pipe for creates", func() {
			err := bbs.ReportActualLongRunningProcessAsRunning(lrp)
			Ω(err).ShouldNot(HaveOccurred())

			Eventually(events).Should(Receive(Equal(lrp)))
		})

		It("sends an event down the pipe for updates", func() {
			err := bbs.ReportActualLongRunningProcessAsRunning(lrp)
			Ω(err).ShouldNot(HaveOccurred())

			Eventually(events).Should(Receive(Equal(lrp)))

			err = bbs.ReportActualLongRunningProcessAsRunning(lrp)
			Ω(err).ShouldNot(HaveOccurred())

			Eventually(events).Should(Receive(Equal(lrp)))
		})

		It("closes the events and errors channel when told to stop", func() {
			stop <- true
			stopped = true

			err := bbs.ReportActualLongRunningProcessAsRunning(lrp)
			Ω(err).ShouldNot(HaveOccurred())

			Ω(events).Should(BeClosed())
			Ω(errors).Should(BeClosed())
		})
	})

	Describe("GetAllDesiredLongRunningProcesses", func() {
		lrp1 := models.DesiredLRP{ProcessGuid: "guid1"}
		lrp2 := models.DesiredLRP{ProcessGuid: "guid2"}
		lrp3 := models.DesiredLRP{ProcessGuid: "guid3"}

		BeforeEach(func() {
			err := bbs.DesireLongRunningProcess(lrp1)
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.DesireLongRunningProcess(lrp2)
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.DesireLongRunningProcess(lrp3)
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("returns all desired long running processes", func() {
			all, err := bbs.GetAllDesiredLongRunningProcesses()
			Ω(err).ShouldNot(HaveOccurred())

			Ω(all).Should(Equal([]models.DesiredLRP{lrp1, lrp2, lrp3}))
		})
	})

	Describe("GetAllActualLongRunningProcesses", func() {
		lrp1 := models.LRP{ProcessGuid: "guid1", Index: 1, InstanceGuid: "some-instance-guid", State: models.LRPStateRunning}
		lrp2 := models.LRP{ProcessGuid: "guid2", Index: 2, InstanceGuid: "some-instance-guid", State: models.LRPStateRunning}
		lrp3 := models.LRP{ProcessGuid: "guid3", Index: 2, InstanceGuid: "some-instance-guid", State: models.LRPStateRunning}

		BeforeEach(func() {
			err := bbs.ReportActualLongRunningProcessAsRunning(lrp1)
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.ReportActualLongRunningProcessAsRunning(lrp2)
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.ReportActualLongRunningProcessAsRunning(lrp3)
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("returns all actual long running processes", func() {
			all, err := bbs.GetAllActualLongRunningProcesses()
			Ω(err).ShouldNot(HaveOccurred())

			Ω(all).Should(Equal([]models.LRP{lrp1, lrp2, lrp3}))
		})
	})
})
