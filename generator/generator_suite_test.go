package generator_test

import (
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
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
	logger  *lagertest.TestLogger
	fakeBBS *fake_bbs.FakeRepBBS
)

var _ = BeforeEach(func() {
	logger = lagertest.NewTestLogger("test")
	fakeBBS = new(fake_bbs.FakeRepBBS)
})
