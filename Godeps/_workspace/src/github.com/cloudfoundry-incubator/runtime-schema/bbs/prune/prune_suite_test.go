package prune_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestPrune(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Prune Suite")
}
