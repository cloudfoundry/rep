package generator_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/executor"
	efakes "github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/generator"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/operationq"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Generator", func() {
	var (
		cellID             string
		fakeExecutorClient *efakes.FakeClient

		opGenerator generator.Generator
	)

	BeforeEach(func() {
		cellID = "some-cell-id"
		fakeExecutorClient = new(efakes.FakeClient)

		opGenerator = generator.New(cellID, fakeBBS, fakeExecutorClient)
	})

	Describe("OperationStream", func() {
		var (
			stream    <-chan operationq.Operation
			streamErr error
		)

		JustBeforeEach(func() {
			stream, streamErr = opGenerator.OperationStream(logger)
		})

		Context("when subscribing to the executor succeeds", func() {
			var receivedEvents chan<- executor.Event

			BeforeEach(func() {
				events := make(chan executor.Event, 1)
				receivedEvents = events

				fakeExecutorClient.SubscribeToEventsReturns(events, nil)
			})

			Context("when a container lifecycle event appears", func() {
				var container executor.Container

				BeforeEach(func() {
					container = executor.Container{
						Guid: "some-instance-guid",

						State: executor.StateCompleted,

						Tags: executor.Tags{
							rep.ProcessGuidTag:  "some-process-guid",
							rep.DomainTag:       "some-domain",
							rep.ProcessIndexTag: "1",
						},
					}
				})

				JustBeforeEach(func() {
					receivedEvents <- executor.NewContainerCompleteEvent(container)
					close(receivedEvents)
				})

				Context("when the lifecycle is LRP", func() {
					BeforeEach(func() {
						container.Tags[rep.LifecycleTag] = rep.LRPLifecycle
					})

					It("yields an operation", func() {
						Eventually(stream).Should(Receive())
					})
				})

				Context("when the lifecycle is Task", func() {
					BeforeEach(func() {
						container.Tags[rep.LifecycleTag] = rep.TaskLifecycle
					})
					var task models.Task

					BeforeEach(func() {
						task = models.Task{
							TaskGuid:   "some-instance-guid",
							State:      models.TaskStateRunning,
							ResultFile: "some-result-file",
						}

						fakeBBS.TaskByGuidReturns(task, nil)
					})

					It("yields an operation", func() {
						Eventually(stream).Should(Receive())
					})
				})
			})
		})

		Context("when subscribing to the executor fails", func() {
			disaster := errors.New("nope")

			BeforeEach(func() {
				fakeExecutorClient.SubscribeToEventsReturns(nil, disaster)
			})

			It("returns the error", func() {
				Ω(streamErr).Should(Equal(disaster))
			})
		})
	})
})
