package fake_gatherer_test

import (
	. "github.com/cloudfoundry-incubator/rep/gatherer/fake_gatherer"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("FakeGatherer", func() {
	var snapshot FakeSnapshot

	It("supports Snapshot", func() {
		Ω(snapshot).ShouldNot(BeNil())
	})
})
