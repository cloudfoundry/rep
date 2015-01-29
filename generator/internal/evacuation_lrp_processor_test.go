package internal_test

import (
	"strconv"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context/fake_evacuation_context"
	"github.com/cloudfoundry-incubator/rep/generator/internal"
	"github.com/cloudfoundry-incubator/rep/generator/internal/fake_internal"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/clock/fakeclock"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EvacuationLrpProcessor", func() {
	Describe("Process", func() {
		const (
			localCellID = "cell-α"
		)

		var (
			clock                  *fakeclock.FakeClock
			logger                 *lagertest.TestLogger
			fakeContainerDelegate  *fake_internal.FakeContainerDelegate
			fakeEvacuationReporter *fake_evacuation_context.FakeEvacuationReporter

			lrpProcessor internal.LRPProcessor

			processGuid  string
			desiredLRP   models.DesiredLRP
			index        int
			container    executor.Container
			instanceGuid string

			lrpKey models.ActualLRPKey
		)

		BeforeEach(func() {
			etcdRunner.Stop()
			etcdRunner.Start()

			clock = fakeclock.NewFakeClock(time.Unix(0, 1138))
			logger = lagertest.NewTestLogger("test")

			BBS = bbs.NewBBS(etcdClient, clock, logger)

			processGuid = "process-guid"
			desiredLRP = models.DesiredLRP{
				Domain:      "domain",
				ProcessGuid: processGuid,
				Instances:   1,
				Stack:       "some-stack",
				Action: &models.RunAction{
					Path: "/bin/true",
				},
			}

			fakeContainerDelegate = &fake_internal.FakeContainerDelegate{}
			fakeEvacuationReporter = &fake_evacuation_context.FakeEvacuationReporter{}
			fakeEvacuationReporter.EvacuatingReturns(true)

			instanceGuid = "instance-guid"
			index = 0

			container = executor.Container{
				Guid: instanceGuid,
				Tags: executor.Tags{
					rep.LifecycleTag:    rep.LRPLifecycle,
					rep.DomainTag:       desiredLRP.Domain,
					rep.ProcessGuidTag:  desiredLRP.ProcessGuid,
					rep.ProcessIndexTag: strconv.Itoa(index),
				},
			}

			lrpKey = models.NewActualLRPKey(processGuid, index, desiredLRP.Domain)
		})

		JustBeforeEach(func() {
			lrpProcessor = internal.NewLRPProcessor(BBS, fakeContainerDelegate, localCellID, fakeEvacuationReporter)
			lrpProcessor.Process(logger, container)
		})

		Context("when the container is Reserved", func() {
			BeforeEach(func() {
				container.State = executor.StateReserved
				err := BBS.DesireLRP(logger, desiredLRP)
				Ω(err).ShouldNot(HaveOccurred())

				lrpContainerKey := models.NewActualLRPContainerKey(instanceGuid, localCellID)
				err = BBS.ClaimActualLRP(lrpKey, lrpContainerKey, logger)
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("unclaims the ActualLRP", func() {
				actualLRP, err := BBS.ActualLRPByProcessGuidAndIndex(processGuid, index)
				Ω(err).ShouldNot(HaveOccurred())

				Ω(actualLRP.State).Should(Equal(models.ActualLRPStateUnclaimed))
			})

			It("deletes the container", func() {
				Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
				_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
				Ω(actualContainerGuid).Should(Equal(instanceGuid))
			})
		})

		Context("when the container is Initializing", func() {
			BeforeEach(func() {
				container.State = executor.StateInitializing
				err := BBS.DesireLRP(logger, desiredLRP)
				Ω(err).ShouldNot(HaveOccurred())

				lrpContainerKey := models.NewActualLRPContainerKey(instanceGuid, localCellID)
				err = BBS.ClaimActualLRP(lrpKey, lrpContainerKey, logger)
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("unclaims the ActualLRP", func() {
				actualLRP, err := BBS.ActualLRPByProcessGuidAndIndex(processGuid, index)
				Ω(err).ShouldNot(HaveOccurred())

				Ω(actualLRP.State).Should(Equal(models.ActualLRPStateUnclaimed))
			})

			It("deletes the container", func() {
				Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
				_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
				Ω(actualContainerGuid).Should(Equal(instanceGuid))
			})
		})

		Context("when the container is Created", func() {
			BeforeEach(func() {
				container.State = executor.StateCreated
				err := BBS.DesireLRP(logger, desiredLRP)
				Ω(err).ShouldNot(HaveOccurred())

				lrpContainerKey := models.NewActualLRPContainerKey(instanceGuid, localCellID)
				err = BBS.ClaimActualLRP(lrpKey, lrpContainerKey, logger)
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("unclaims the ActualLRP", func() {
				actualLRP, err := BBS.ActualLRPByProcessGuidAndIndex(processGuid, index)
				Ω(err).ShouldNot(HaveOccurred())

				Ω(actualLRP.State).Should(Equal(models.ActualLRPStateUnclaimed))
			})

			It("deletes the container", func() {
				Ω(fakeContainerDelegate.DeleteContainerCallCount()).Should(Equal(1))
				_, actualContainerGuid := fakeContainerDelegate.DeleteContainerArgsForCall(0)
				Ω(actualContainerGuid).Should(Equal(instanceGuid))
			})
		})
	})
})
