package auction_cell_rep_test

import (
	"code.cloudfoundry.org/rep/auction_cell_rep"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Helpers", func() {
	Context("GenerateGuid", func() {
		It("generates a 28 character identifier", func() {
			guid, err := auction_cell_rep.GenerateGuid()
			Expect(err).NotTo(HaveOccurred())
			Expect(guid).To(HaveLen(28))
		})
	})
})
