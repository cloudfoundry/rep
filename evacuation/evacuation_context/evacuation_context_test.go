package evacuation_context_test

import (
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EvacuationContext", func() {
	var (
		evacuatable        evacuation_context.Evacuatable
		evacuationReporter evacuation_context.EvacuationReporter
	)

	BeforeEach(func() {
		evacuatable, evacuationReporter = evacuation_context.New()
	})

	Describe("Evacuatable", func() {
		Context("when Evacuate has not been called", func() {
			It("does not make the evacuationReporter return true for Evacuating", func() {
				Ω(evacuationReporter.Evacuating()).Should(BeFalse())
			})
		})

		Context("when Evacuate has been called", func() {
			BeforeEach(func() {
				evacuatable.Evacuate()
			})

			It("makes the evacuationReporter return true for Evacuating", func() {
				Ω(evacuationReporter.Evacuating()).Should(BeTrue())
			})

			Context("when Evacuate is called again", func() {
				BeforeEach(func() {
					evacuatable.Evacuate()
				})

				It("makes the evacuationReporter return true for Evacuating", func() {
					Ω(evacuationReporter.Evacuating()).Should(BeTrue())
				})
			})
		})
	})
})
