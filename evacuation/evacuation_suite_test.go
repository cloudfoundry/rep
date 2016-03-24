package evacuation_test

import (
	"github.com/cloudfoundry-incubator/bbs/fake_bbs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

var (
	fakeBBSClient *fake_bbs.FakeClient
)

var _ = BeforeEach(func() {
	fakeBBSClient = new(fake_bbs.FakeClient)
})

func TestEvacuation(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Evacuation Suite")
}
