package harvester_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/executor/fakes"
	. "github.com/cloudfoundry-incubator/rep/harvester"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/pivotal-golang/archiver/extractor/test_helper"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("Processor", func() {
	var (
		executorClient *fakes.FakeClient
		bbs            *fake_bbs.FakeRepBBS

		processor Processor

		completedContainer executor.Container
	)

	BeforeEach(func() {
		executorClient = new(fakes.FakeClient)
		bbs = new(fake_bbs.FakeRepBBS)

		processor = NewProcessor(lagertest.NewTestLogger("test"), bbs, executorClient)

		completedContainer = executor.Container{
			Guid:  "completed-guid",
			State: executor.StateCompleted,
		}
	})

	JustBeforeEach(func() {
		processor.Process(completedContainer)
	})

	Context("when the container completed successfully", func() {
		BeforeEach(func() {
			completedContainer.RunResult = executor.ContainerRunResult{
				Failed: false,
			}
		})

		Context("when the completed container has a result file", func() {
			BeforeEach(func() {
				completedContainer.Tags = executor.Tags{
					ResultFileTag: "some-result-file",
				}
			})

			Context("and getting the result from the container succeeds", func() {
				BeforeEach(func() {
					dest := gbytes.NewBuffer()

					test_helper.WriteTar(
						dest,
						[]test_helper.ArchiveFile{{
							Name: "some-file",
							Body: "something",
							Mode: 0600,
							Dir:  false,
						}},
					)

					executorClient.GetFilesReturns(dest, nil)
				})

				It("gets the result file described by the container's metadata", func() {
					Ω(executorClient.GetFilesCallCount()).Should(Equal(1))

					guid, path := executorClient.GetFilesArgsForCall(0)
					Ω(guid).Should(Equal("completed-guid"))
					Ω(path).Should(Equal("some-result-file"))
				})

				It("completes the task successfully with the result", func() {
					Ω(bbs.CompleteTaskCallCount()).Should(Equal(1))

					taskGuid, failed, failureReason, result := bbs.CompleteTaskArgsForCall(0)
					Ω(taskGuid).Should(Equal("completed-guid"))
					Ω(failed).Should(BeFalse())
					Ω(failureReason).Should(BeEmpty())
					Ω(result).Should(Equal("something"))
				})
			})

			Context("and getting the result from the container fails", func() {
				disaster := errors.New("oh no!")

				BeforeEach(func() {
					executorClient.GetFilesReturns(nil, disaster)
				})

				It("completes the task with failure", func() {
					Ω(bbs.CompleteTaskCallCount()).Should(Equal(1))

					taskGuid, failed, failureReason, result := bbs.CompleteTaskArgsForCall(0)
					Ω(taskGuid).Should(Equal("completed-guid"))
					Ω(failed).Should(BeTrue())
					Ω(failureReason).Should(Equal("failed to fetch result: oh no!"))
					Ω(result).Should(BeEmpty())
				})
			})
		})
	})

	Context("when the container completed with failure", func() {
		BeforeEach(func() {
			completedContainer.RunResult = executor.ContainerRunResult{
				Failed:        true,
				FailureReason: "shiitake mushrooms are happening",
			}
		})

		It("completes the task with failure", func() {
			Ω(bbs.CompleteTaskCallCount()).Should(Equal(1))

			taskGuid, failed, failureReason, result := bbs.CompleteTaskArgsForCall(0)
			Ω(taskGuid).Should(Equal("completed-guid"))
			Ω(failed).Should(BeTrue())
			Ω(failureReason).Should(Equal("shiitake mushrooms are happening"))
			Ω(result).Should(BeEmpty())
		})

		It("deletes the container", func() {
			Ω(executorClient.DeleteContainerCallCount()).Should(Equal(1))
			Ω(executorClient.DeleteContainerArgsForCall(0)).Should(Equal("completed-guid"))
		})

		Context("when the completed container has a result file", func() {
			BeforeEach(func() {
				completedContainer.Tags = executor.Tags{
					ResultFileTag: "some-result-file",
				}
			})

			It("does not try to fetch the result", func() {
				Ω(executorClient.GetFilesCallCount()).Should(BeZero())
			})
		})
	})
})
