package snapshot_test

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep/snapshot"
	"github.com/cloudfoundry-incubator/runtime-schema/models"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var exampleLRP = snapshot.NewLRP(models.NewActualLRPKey("process-guid", 0, "domain"), models.NewActualLRPContainerKey("instance-guid", "cell-id"))
var exampleTask = snapshot.NewTask("task-guid", "cell-id", models.TaskStateRunning, "some-result-filename")
var exampleContainer = &executor.Container{Guid: "some-guid"}

var _ = Describe("Snapshot Data Types", func() {
	Describe("LRPSnapshot", func() {
		Describe("Validate", func() {
			var validationTestInput = map[*snapshot.LRPSnapshot]error{
				snapshot.NewLRPSnapshot(nil, nil):                     snapshot.ErrEmptyLRPSnapshot,
				snapshot.NewLRPSnapshot(nil, exampleContainer):        snapshot.ErrLRPSnapshotMissingLRP,
				snapshot.NewLRPSnapshot(exampleLRP, nil):              nil,
				snapshot.NewLRPSnapshot(exampleLRP, exampleContainer): nil,
			}

			It("returns the correct error when the snapshot is invalid", func() {
				for snap, result := range validationTestInput {
					if result == nil {
						Ω(snap.Validate()).Should(BeNil())
					} else {
						Ω(snap.Validate()).Should(Equal(result))
					}
				}
			})
		})
	})

	Describe("TaskSnapshot", func() {
		Describe("Validate", func() {
			var validationTestInput = map[*snapshot.TaskSnapshot]error{
				snapshot.NewTaskSnapshot(nil, nil):                      snapshot.ErrEmptyTaskSnapshot,
				snapshot.NewTaskSnapshot(nil, exampleContainer):         nil,
				snapshot.NewTaskSnapshot(exampleTask, nil):              nil,
				snapshot.NewTaskSnapshot(exampleTask, exampleContainer): nil,
			}

			It("returns the correct error when the snapshot is invalid", func() {
				for snap, result := range validationTestInput {
					if result == nil {
						Ω(snap.Validate()).Should(BeNil())
					} else {
						Ω(snap.Validate()).Should(Equal(result))
					}
				}
			})
		})
	})
})
