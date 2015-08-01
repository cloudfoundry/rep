package rep_test

import (
	"time"

	"github.com/cloudfoundry-incubator/cf_http"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

var cfHttpTimeout time.Duration

func TestRep(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rep Suite")
}

var _ = BeforeSuite(func() {
	cfHttpTimeout = 1 * time.Second
	cf_http.Initialize(cfHttpTimeout)
})
