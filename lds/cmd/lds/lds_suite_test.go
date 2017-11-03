package main_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"testing"
)

var ldsBinPath string

func TestLds(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Lds Suite")
}

var _ = SynchronizedBeforeSuite(
	func() []byte {
		ldsBinPathData, err := gexec.Build("code.cloudfoundry.org/rep/lds/cmd/lds", "-race")
		Expect(err).NotTo(HaveOccurred())
		return []byte(ldsBinPathData)
	},
	func(ldsBinPathData []byte) {
		ldsBinPath = string(ldsBinPathData)
	})

var _ = SynchronizedAfterSuite(func() {}, func() {
	gexec.CleanupBuildArtifacts()
})
