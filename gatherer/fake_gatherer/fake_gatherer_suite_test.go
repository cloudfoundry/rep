package fake_gatherer_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestFakeGatherer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FakeGatherer Suite")
}
