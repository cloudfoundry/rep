package heartbeater_test

import (
	"fmt"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/http_server"

	"testing"
)

var proxyRunner ifrit.Runner
var proxyUrl string

var etcdRunner *etcdstorerunner.ETCDClusterRunner
var etcdPort int

func TestHeartbeater(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Heartbeater Suite")
}

var _ = BeforeSuite(func() {
	etcdPort = 5001 + config.GinkgoConfig.ParallelNode
	etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1)

	proxyUrl = fmt.Sprintf("http://127.0.0.1:%d", 6001+config.GinkgoConfig.ParallelNode)
	proxyRunner = newEtcdProxy(proxyUrl, etcdPort)
})

var _ = AfterSuite(func() {
	etcdRunner.Stop()
})

func newEtcdProxy(proxyUrl string, etcdPort int) ifrit.Runner {
	etcdUrl := &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("127.0.0.1:%d", etcdPort),
	}

	proxyHandler := httputil.NewSingleHostReverseProxy(etcdUrl)

	return http_server.New(strings.TrimLeft(proxyUrl, "http://"), proxyHandler)
}
