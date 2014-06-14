package stop_lrp_listener_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestStopLRPListener(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "StopLRPListener Suite")
}
