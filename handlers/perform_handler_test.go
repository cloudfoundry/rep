package handlers_test

import (
	"bytes"
	"errors"
	"net/http"

	"github.com/cloudfoundry-incubator/rep"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Perform", func() {
	Context("with valid JSON", func() {
		var requestedWork, failedWork rep.Work
		BeforeEach(func() {
			resourceA := rep.NewResource(128, 256, "some-rootfs")
			resourceB := rep.NewResource(256, 512, "some-rootfs")
			resourceC := rep.NewResource(512, 1024, "some-rootfs")

			requestedWork = rep.Work{
				Tasks: []rep.Task{
					rep.NewTask("a", "domain", resourceA),
					rep.NewTask("b", "domain", resourceB),
				},
			}

			failedWork = rep.Work{
				Tasks: []rep.Task{
					rep.NewTask("c", "domain", resourceC),
				},
			}
		})

		Context("and no perform error", func() {
			BeforeEach(func() {
				fakeLocalRep.PerformReturns(failedWork, nil)
			})

			It("succeeds, returning any failed work", func() {
				Expect(fakeLocalRep.PerformCallCount()).To(Equal(0))

				status, body := Request(rep.PerformRoute, nil, JSONReaderFor(requestedWork))
				Expect(status).To(Equal(http.StatusOK))
				Expect(body).To(MatchJSON(JSONFor(failedWork)))

				Expect(fakeLocalRep.PerformCallCount()).To(Equal(1))
				Expect(fakeLocalRep.PerformArgsForCall(0)).To(Equal(requestedWork))
			})
		})

		Context("and a perform error", func() {
			BeforeEach(func() {
				fakeLocalRep.PerformReturns(failedWork, errors.New("kaboom"))
			})

			It("fails, returning nothing", func() {
				Expect(fakeLocalRep.PerformCallCount()).To(Equal(0))

				status, body := Request(rep.PerformRoute, nil, JSONReaderFor(requestedWork))
				Expect(status).To(Equal(http.StatusInternalServerError))
				Expect(body).To(BeEmpty())

				Expect(fakeLocalRep.PerformCallCount()).To(Equal(1))
				Expect(fakeLocalRep.PerformArgsForCall(0)).To(Equal(requestedWork))
			})
		})
	})

	Context("with invalid JSON", func() {
		It("fails", func() {
			Expect(fakeLocalRep.PerformCallCount()).To(Equal(0))

			status, body := Request(rep.PerformRoute, nil, bytes.NewBufferString("∆"))
			Expect(status).To(Equal(http.StatusBadRequest))
			Expect(body).To(BeEmpty())

			Expect(fakeLocalRep.PerformCallCount()).To(Equal(0))
		})
	})
})
