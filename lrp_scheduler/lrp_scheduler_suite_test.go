package lrp_scheduler_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestLrpScheduler(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "LrpScheduler Suite")
}
