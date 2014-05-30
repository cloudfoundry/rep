package lrp_stopper_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestLrpStopper(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "LrpStopper Suite")
}
