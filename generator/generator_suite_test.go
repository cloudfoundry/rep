package generator_test

import (
	"github.com/cloudfoundry-incubator/bbs/fake_bbs"
	fake_legacy_bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-golang/lager/lagertest"

	"testing"
)

func TestGenerator(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Generator Suite")
}

var (
	logger        *lagertest.TestLogger
	fakeBBS       *fake_bbs.FakeClient
	fakeLegacyBBS *fake_legacy_bbs.FakeRepBBS
)

var _ = BeforeEach(func() {
	logger = lagertest.NewTestLogger("test")
	fakeBBS = new(fake_bbs.FakeClient)
	fakeLegacyBBS = new(fake_legacy_bbs.FakeRepBBS)
})
