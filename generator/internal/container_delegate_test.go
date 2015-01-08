package internal_test

import (
	"errors"
	"strings"

	"github.com/cloudfoundry-incubator/executor/fakes"
	"github.com/cloudfoundry-incubator/rep/generator/internal"
	"github.com/pivotal-golang/archiver/extractor/test_helper"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("ContainerDelegate", func() {
	var containerDelegate internal.ContainerDelegate
	var executorClient *fakes.FakeClient
	var logger *lagertest.TestLogger
	var expectedGuid = "some-instance-guid"
	const sessionPrefix = "test"

	BeforeEach(func() {
		executorClient = new(fakes.FakeClient)
		containerDelegate = internal.NewContainerDelegate(executorClient)
		logger = lagertest.NewTestLogger(sessionPrefix)
	})

	Describe("RunContainer", func() {
		var result bool

		JustBeforeEach(func() {
			result = containerDelegate.RunContainer(logger, expectedGuid)
		})

		It("runs the container", func() {
			Ω(executorClient.RunContainerCallCount()).Should(Equal(1))
			containerGuid := executorClient.RunContainerArgsForCall(0)
			Ω(containerGuid).Should(Equal(expectedGuid))
		})

		Context("when running succeeds", func() {
			It("returns true", func() {
				Ω(result).Should(BeTrue())
			})

			It("logs the running", func() {
				Ω(logger).Should(gbytes.Say(sessionPrefix + ".running-container"))
				Ω(logger).Should(gbytes.Say(sessionPrefix + ".succeeded-running-container"))
			})
		})

		Context("when running fails", func() {
			BeforeEach(func() {
				executorClient.RunContainerReturns(errors.New("ka-boom"))
			})

			It("returns false", func() {
				Ω(result).Should(BeFalse())
			})

			It("logs the failure", func() {
				Ω(logger).Should(gbytes.Say(sessionPrefix + ".failed-running-container"))
			})

			It("deletes the container", func() {
				Ω(executorClient.DeleteContainerCallCount()).Should(Equal(1))
				containerGuid := executorClient.DeleteContainerArgsForCall(0)
				Ω(containerGuid).Should(Equal(expectedGuid))
			})

			It("logs the deletion", func() {
				Ω(logger).Should(gbytes.Say(sessionPrefix + ".deleting-container"))
				Ω(logger).Should(gbytes.Say(sessionPrefix + ".succeeded-deleting-container"))
			})

			Context("when deleting fails", func() {
				BeforeEach(func() {
					executorClient.DeleteContainerReturns(errors.New("boom"))
				})

				It("logs the failure", func() {
					Ω(logger).Should(gbytes.Say(sessionPrefix + ".failed-deleting-container"))
				})
			})
		})
	})

	Describe("StopContainer", func() {
		var result bool

		JustBeforeEach(func() {
			result = containerDelegate.StopContainer(logger, expectedGuid)
		})

		It("stops the container", func() {
			Ω(executorClient.StopContainerCallCount()).Should(Equal(1))
			containerGuid := executorClient.StopContainerArgsForCall(0)
			Ω(containerGuid).Should(Equal(expectedGuid))
		})

		Context("when stopping succeeds", func() {
			It("returns true", func() {
				Ω(result).Should(BeTrue())
			})

			It("logs the stopping", func() {
				Ω(logger).Should(gbytes.Say(sessionPrefix + ".stopping-container"))
				Ω(logger).Should(gbytes.Say(sessionPrefix + ".succeeded-stopping-container"))
			})
		})

		Context("when stopping fails", func() {
			BeforeEach(func() {
				executorClient.StopContainerReturns(errors.New("ka-boom"))
			})

			It("returns false", func() {
				Ω(result).Should(BeFalse())
			})

			It("logs the failure", func() {
				Ω(logger).Should(gbytes.Say(sessionPrefix + ".failed-stopping-container"))
			})
		})
	})

	Describe("DeleteContainer", func() {
		var result bool

		JustBeforeEach(func() {
			result = containerDelegate.DeleteContainer(logger, expectedGuid)
		})

		It("deletes the container", func() {
			Ω(executorClient.DeleteContainerCallCount()).Should(Equal(1))
			containerGuid := executorClient.DeleteContainerArgsForCall(0)
			Ω(containerGuid).Should(Equal(expectedGuid))
		})

		Context("when deleting succeeds", func() {
			It("returns true", func() {
				Ω(result).Should(BeTrue())
			})

			It("logs the deleting", func() {
				Ω(logger).Should(gbytes.Say(sessionPrefix + ".deleting-container"))
				Ω(logger).Should(gbytes.Say(sessionPrefix + ".succeeded-deleting-container"))
			})
		})

		Context("when deleting fails", func() {
			BeforeEach(func() {
				executorClient.DeleteContainerReturns(errors.New("ka-boom"))
			})

			It("returns false", func() {
				Ω(result).Should(BeFalse())
			})

			It("logs the failure", func() {
				Ω(logger).Should(gbytes.Say(sessionPrefix + ".failed-deleting-container"))
			})
		})
	})

	Describe("FetchContainerResult", func() {
		var (
			filename string

			result   string
			fetchErr error
		)

		BeforeEach(func() {
			filename = "some-filename"
		})

		JustBeforeEach(func() {
			result, fetchErr = containerDelegate.FetchContainerResult(logger, expectedGuid, filename)
		})

		Context("when fetching the file stream from the container succeeds", func() {
			var fileStream *gbytes.Buffer

			BeforeEach(func() {
				fileStream = gbytes.NewBuffer()
				executorClient.GetFilesReturns(fileStream, nil)
			})

			Context("and the payload is a reasonable size", func() {
				BeforeEach(func() {
					test_helper.WriteTar(
						fileStream,
						[]test_helper.ArchiveFile{{
							Name: "some-file",
							Body: "some result",
							Mode: 0600,
						}},
					)
				})

				It("succeeds", func() {
					Ω(fetchErr).ShouldNot(HaveOccurred())
				})

				It("returns the result of the first file in the stream", func() {
					Ω(result).Should(Equal("some result"))
				})

				It("closes the result stream", func() {
					Ω(fileStream.Closed()).Should(BeTrue())
				})
			})

			Context("but the payload is too large", func() {
				BeforeEach(func() {
					test_helper.WriteTar(
						fileStream,
						[]test_helper.ArchiveFile{{
							Name: "some-file",
							Body: strings.Repeat("x", internal.MAX_RESULT_SIZE+1),
							Mode: 0600,
						}},
					)
				})

				It("returns an error", func() {
					Ω(fetchErr).Should(HaveOccurred())
				})

				It("closes the result stream", func() {
					Ω(fileStream.Closed()).Should(BeTrue())
				})
			})

			Context("when the stream is empty for whatever reason", func() {
				It("returns an error", func() {
					Ω(fetchErr).Should(HaveOccurred())
				})

				It("closes the result stream", func() {
					Ω(fileStream.Closed()).Should(BeTrue())
				})
			})
		})

		Context("when fetching the file stream from the container fails", func() {
			disaster := errors.New("nope")

			BeforeEach(func() {
				executorClient.GetFilesReturns(nil, disaster)
			})

			It("returns the error", func() {
				Ω(fetchErr).Should(Equal(disaster))
			})
		})
	})
})
