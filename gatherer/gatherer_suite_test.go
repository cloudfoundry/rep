package gatherer_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestGatherer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Gatherer Suite")
}
