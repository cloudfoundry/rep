package harvester_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestHarvester(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Harvester Suite")
}
