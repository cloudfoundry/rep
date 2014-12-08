package fake_gatherer_test

import (
	_ "github.com/cloudfoundry-incubator/rep/gatherer/fake_gatherer"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("FakeGatherer", func() {
	// will fail to compile in fake implementation if concrete fake cannot be assigned to faked interface
})
