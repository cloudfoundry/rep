package snapshot_test

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep/snapshot"
	"github.com/cloudfoundry-incubator/rep/snapshot/fake_snapshot"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("SnapshotProcessor", func() {
	var (
		processor     snapshot.SnapshotProcessor
		lrpProcessor  *fake_snapshot.FakeLRPProcessor
		taskProcessor *fake_snapshot.FakeTaskProcessor
	)

	BeforeEach(func() {
		lrpProcessor = new(fake_snapshot.FakeLRPProcessor)
		taskProcessor = new(fake_snapshot.FakeTaskProcessor)
		processor = snapshot.NewSnapshotProcessor(lrpProcessor, taskProcessor)
	})

	Describe("Process", func() {
		var logger *lagertest.TestLogger
		var snap snapshot.Snapshot

		BeforeEach(func() {
			logger = lagertest.NewTestLogger("test")
		})

		JustBeforeEach(func() {
			processor.Process(logger, snap)
		})

		Context("when the snapshot is an LRPSnapshot", func() {
			BeforeEach(func() {
				guid := "some-guid"
				lrp := snapshot.NewLRP(models.NewActualLRPKey("process-id", 0, "domain"), models.NewActualLRPContainerKey(guid, "cell"))
				container := &executor.Container{
					Guid: guid,
				}
				snap = snapshot.NewLRPSnapshot(lrp, container)
			})

			It("processes the snapshot via the lrp processor", func() {
				Ω(lrpProcessor.ProcessCallCount()).Should(Equal(1))
				processedLogger, processedSnap := lrpProcessor.ProcessArgsForCall(0)
				Ω(processedLogger.SessionName()).Should(ContainSubstring("test.container-processor"))
				Ω(processedSnap).Should(Equal(snap))
			})
		})

		Context("when the snapshot is a TaskSnapshot", func() {
			BeforeEach(func() {
				guid := "some-guid"
				task := snapshot.NewTask(guid, "cell", models.TaskStateRunning, "some-result-filename")
				container := &executor.Container{
					Guid: guid,
				}

				snap = snapshot.NewTaskSnapshot(task, container)
			})

			It("processes the snapshot via the task processor", func() {
				Ω(taskProcessor.ProcessCallCount()).Should(Equal(1))
				processedLogger, processedSnap := taskProcessor.ProcessArgsForCall(0)
				Ω(processedLogger.SessionName()).Should(ContainSubstring("test.container-processor"))
				Ω(processedSnap).Should(Equal(snap))
			})
		})

		Context("when the snapshot is inconceivable", func() {
			BeforeEach(func() {
				snap = new(fake_snapshot.FakeSnapshot)
			})

			It("logs", func() {
				Ω(logger).Should(gbytes.Say(snapshot.ErrInconceivableSnapshotType.Error()))
			})
		})
	})
})
