package lrp_bbs_test

import (
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/lrp_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LrpGetters", func() {
	var lrp1, lrp2, lrp3 models.ActualLRP
	var desiredLrp1, desiredLrp2, desiredLrp3 models.DesiredLRP

	BeforeEach(func() {
		desiredLrp1 = models.DesiredLRP{
			ProcessGuid: "guidA",
			Stack:       "stack",
			Actions: []models.ExecutorAction{
				{
					Action: models.DownloadAction{
						From: "http://example.com",
						To:   "/tmp/internet",
					},
				},
			},
		}

		desiredLrp2 = models.DesiredLRP{
			ProcessGuid: "guidB",
			Stack:       "stack",
			Actions: []models.ExecutorAction{
				{
					Action: models.DownloadAction{
						From: "http://example.com",
						To:   "/tmp/internet",
					},
				},
			},
		}

		desiredLrp3 = models.DesiredLRP{
			ProcessGuid: "guidC",
			Stack:       "stack",
			Actions: []models.ExecutorAction{
				{
					Action: models.DownloadAction{
						From: "http://example.com",
						To:   "/tmp/internet",
					},
				},
			},
		}

		lrp1 = models.ActualLRP{
			ProcessGuid:  "guidA",
			Index:        1,
			InstanceGuid: "some-instance-guid",
			State:        models.ActualLRPStateRunning,
			Since:        timeProvider.Time().UnixNano(),
			ExecutorID:   "executor-id",
		}

		lrp2 = models.ActualLRP{
			ProcessGuid:  "guidA",
			Index:        2,
			InstanceGuid: "some-instance-guid",
			State:        models.ActualLRPStateStarting,
			Since:        timeProvider.Time().UnixNano(),
			ExecutorID:   "executor-id",
		}

		lrp3 = models.ActualLRP{
			ProcessGuid:  "guidB",
			Index:        2,
			InstanceGuid: "some-instance-guid",
			State:        models.ActualLRPStateRunning,
			Since:        timeProvider.Time().UnixNano(),
			ExecutorID:   "executor-id",
		}
	})

	Describe("GetAllDesiredLRPs", func() {
		BeforeEach(func() {
			err := bbs.DesireLRP(desiredLrp1)
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.DesireLRP(desiredLrp2)
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.DesireLRP(desiredLrp3)
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("returns all desired long running processes", func() {
			all, err := bbs.GetAllDesiredLRPs()
			Ω(err).ShouldNot(HaveOccurred())

			Ω(all).Should(HaveLen(3))
			Ω(all).Should(ContainElement(desiredLrp1))
			Ω(all).Should(ContainElement(desiredLrp2))
			Ω(all).Should(ContainElement(desiredLrp3))
		})
	})

	Describe("GetAllDesiredLRPsByDomain", func() {
		BeforeEach(func() {
			desiredLrp1.Domain = "domain-1"
			desiredLrp2.Domain = "domain-1"
			desiredLrp3.Domain = "domain-2"

			err := bbs.DesireLRP(desiredLrp1)
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.DesireLRP(desiredLrp2)
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.DesireLRP(desiredLrp3)
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("returns all desired long running processes for the given domain", func() {
			byDomain, err := bbs.GetAllDesiredLRPsByDomain("domain-1")
			Ω(err).ShouldNot(HaveOccurred())
			Ω(byDomain).Should(ConsistOf([]models.DesiredLRP{desiredLrp1, desiredLrp2}))

			byDomain, err = bbs.GetAllDesiredLRPsByDomain("domain-2")
			Ω(err).ShouldNot(HaveOccurred())
			Ω(byDomain).Should(ConsistOf([]models.DesiredLRP{desiredLrp3}))
		})

		It("blows up with an empty string domain", func() {
			_, err := bbs.GetAllDesiredLRPsByDomain("")
			Ω(err).Should(Equal(lrp_bbs.ErrNoDomain))
		})
	})

	Describe("GetDesiredLRPByProcessGuid", func() {
		BeforeEach(func() {
			err := bbs.DesireLRP(desiredLrp1)
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.DesireLRP(desiredLrp2)
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.DesireLRP(desiredLrp3)
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("returns all desired long running processes", func() {
			desiredLrp, err := bbs.GetDesiredLRPByProcessGuid("guidA")
			Ω(err).ShouldNot(HaveOccurred())

			Ω(desiredLrp).Should(Equal(desiredLrp1))
		})
	})

	Describe("GetAllActualLRPs", func() {
		BeforeEach(func() {
			err := bbs.ReportActualLRPAsRunning(lrp1, "executor-id")
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.ReportActualLRPAsStarting(lrp2, "executor-id")
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.ReportActualLRPAsRunning(lrp3, "executor-id")
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("returns all actual long running processes", func() {
			all, err := bbs.GetAllActualLRPs()
			Ω(err).ShouldNot(HaveOccurred())

			Ω(all).Should(HaveLen(3))
			Ω(all).Should(ContainElement(lrp1))
			Ω(all).Should(ContainElement(lrp2))
			Ω(all).Should(ContainElement(lrp3))
		})
	})

	Describe("GetRunningActualLRPs", func() {
		BeforeEach(func() {
			err := bbs.ReportActualLRPAsRunning(lrp1, "executor-id")
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.ReportActualLRPAsStarting(lrp2, "executor-id")
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.ReportActualLRPAsRunning(lrp3, "executor-id")
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("returns all actual long running processes", func() {
			all, err := bbs.GetRunningActualLRPs()
			Ω(err).ShouldNot(HaveOccurred())

			Ω(all).Should(HaveLen(2))
			Ω(all).Should(ContainElement(lrp1))
			Ω(all).Should(ContainElement(lrp3))
		})
	})

	Describe("GetActualLRPsByProcessGuid", func() {
		BeforeEach(func() {
			err := bbs.ReportActualLRPAsRunning(lrp1, "executor-id")
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.ReportActualLRPAsStarting(lrp2, "executor-id")
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.ReportActualLRPAsRunning(lrp3, "executor-id")
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("should fetch all LRPs for the specified guid", func() {
			lrps, err := bbs.GetActualLRPsByProcessGuid("guidA")
			Ω(err).ShouldNot(HaveOccurred())
			Ω(lrps).Should(HaveLen(2))
			Ω(lrps).Should(ContainElement(lrp1))
			Ω(lrps).Should(ContainElement(lrp2))
		})
	})

	Describe("GetRunningActualLRPsByProcessGuid", func() {
		BeforeEach(func() {
			err := bbs.ReportActualLRPAsRunning(lrp1, "executor-id")
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.ReportActualLRPAsStarting(lrp2, "executor-id")
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.ReportActualLRPAsRunning(lrp3, "executor-id")
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("should fetch all LRPs for the specified guid", func() {
			lrps, err := bbs.GetRunningActualLRPsByProcessGuid("guidA")
			Ω(err).ShouldNot(HaveOccurred())
			Ω(lrps).Should(HaveLen(1))
			Ω(lrps).Should(ContainElement(lrp1))
		})
	})
})
