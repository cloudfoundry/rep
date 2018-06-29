package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"runtime"
	"strings"
	"time"

	diego_logging_client "code.cloudfoundry.org/diego-logging-client"

	"code.cloudfoundry.org/bbs"
	bbstestrunner "code.cloudfoundry.org/bbs/cmd/bbs/testrunner"
	"code.cloudfoundry.org/bbs/models"
	"code.cloudfoundry.org/bbs/models/test/model_helpers"
	"code.cloudfoundry.org/cfhttp"
	"code.cloudfoundry.org/durationjson"
	"code.cloudfoundry.org/executor/gardenhealth"
	executorinit "code.cloudfoundry.org/executor/initializer"
	"code.cloudfoundry.org/garden"
	"code.cloudfoundry.org/garden/transport"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagerflags"
	"code.cloudfoundry.org/lager/lagertest"
	"code.cloudfoundry.org/locket"
	locketconfig "code.cloudfoundry.org/locket/cmd/locket/config"
	locketrunner "code.cloudfoundry.org/locket/cmd/locket/testrunner"
	locketmodels "code.cloudfoundry.org/locket/models"
	"code.cloudfoundry.org/rep"
	"code.cloudfoundry.org/rep/cmd/rep/config"
	"code.cloudfoundry.org/rep/cmd/rep/testrunner"

	"github.com/hashicorp/consul/api"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	. "github.com/onsi/gomega/gexec"
	"github.com/onsi/gomega/ghttp"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"
)

var runner *testrunner.Runner

var _ = Describe("The Rep", func() {
	var (
		repConfig                           config.RepConfig
		fakeGarden                          *ghttp.Server
		pollingInterval                     time.Duration
		evacuationTimeout                   time.Duration
		rootFSName                          string
		rootFSPath                          string
		logger                              *lagertest.TestLogger
		basePath                            string
		respondWithSuccessToCreateContainer bool
		caFile, certFile, keyFile           string
		client                              *http.Client

		flushEvents chan struct{}
	)

	var getActualLRPGroups = func(logger lager.Logger) func() []*models.ActualLRPGroup {
		return func() []*models.ActualLRPGroup {
			actualLRPGroups, err := bbsClient.ActualLRPGroups(logger, models.ActualLRPFilter{})
			Expect(err).NotTo(HaveOccurred())
			return actualLRPGroups
		}
	}

	BeforeEach(func() {
		basePath = path.Join(os.Getenv("GOPATH"), "src/code.cloudfoundry.org/rep/cmd/rep/fixtures")
		caFile = path.Join(basePath, "green-certs", "server-ca.crt")
		certFile = path.Join(basePath, "green-certs", "server.crt")
		keyFile = path.Join(basePath, "green-certs", "server.key")
		tlsConfig, err := cfhttp.NewTLSConfig(certFile, keyFile, caFile)
		Expect(err).NotTo(HaveOccurred())
		client = &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}

		logger = lagertest.NewTestLogger("test")

		respondWithSuccessToCreateContainer = true

		caFile := path.Join(basePath, "green-certs", "server-ca.crt")
		clientCertFile := path.Join(basePath, "green-certs", "client.crt")
		clientKeyFile := path.Join(basePath, "green-certs", "client.key")
		bbsClient, err = bbs.NewClient(bbsURL.String(), caFile, clientCertFile, clientKeyFile, 0, 0)
		Expect(err).NotTo(HaveOccurred())

		Eventually(getActualLRPGroups(logger), 5*pollingInterval).Should(BeEmpty())
		flushEvents = make(chan struct{})
		fakeGarden = ghttp.NewUnstartedServer()
		// these tests only look for the start of a sequence of requests
		fakeGarden.AllowUnhandledRequests = false
		fakeGarden.RouteToHandler("GET", "/ping", ghttp.RespondWithJSONEncoded(http.StatusOK, struct{}{}))
		fakeGarden.RouteToHandler("GET", "/containers", ghttp.RespondWithJSONEncoded(http.StatusOK, struct{}{}))
		fakeGarden.RouteToHandler("GET", "/capacity", ghttp.RespondWithJSONEncoded(http.StatusOK,
			garden.Capacity{MemoryInBytes: 1024 * 1024 * 1024, DiskInBytes: 20 * 1024 * 1024 * 1024, MaxContainers: 4}))
		fakeGarden.RouteToHandler("GET", "/containers/bulk_info", ghttp.RespondWithJSONEncoded(http.StatusOK, struct{}{}))

		// The following handlers are needed to fake out the healthcheck containers
		fakeGarden.RouteToHandler("DELETE", regexp.MustCompile("/containers/executor-healthcheck-[-a-f0-9]+"), ghttp.RespondWithJSONEncoded(http.StatusOK, struct{}{}))
		fakeGarden.RouteToHandler("POST", "/containers/healthcheck-container/processes", func() http.HandlerFunc {
			firstResponse, err := json.Marshal(transport.ProcessPayload{})
			Expect(err).NotTo(HaveOccurred())

			exitStatus := 0
			secondResponse, err := json.Marshal(transport.ProcessPayload{ExitStatus: &exitStatus})
			Expect(err).NotTo(HaveOccurred())

			headers := http.Header{"Content-Type": []string{"application/json"}}
			response := string(firstResponse) + string(secondResponse)
			return ghttp.RespondWith(http.StatusOK, response, headers)
		}())

		pollingInterval = 50 * time.Millisecond
		evacuationTimeout = 200 * time.Millisecond

		rootFSName = "the-rootfs"
		rootFSPath = "/path/to/rootfs"

		caFile = path.Join(basePath, "green-certs", "server-ca.crt")
		certFile = path.Join(basePath, "green-certs", "server.crt")
		keyFile = path.Join(basePath, "green-certs", "server.key")

		metricsPort, err := testIngressServer.Port()
		Expect(err).NotTo(HaveOccurred())

		metronCAFile := path.Join(fixturesPath, "metron", "CA.crt")
		metronClientCertFile := path.Join(fixturesPath, "metron", "client.crt")
		metronClientKeyFile := path.Join(fixturesPath, "metron", "client.key")

		repConfig = config.RepConfig{
			PreloadedRootFS:       []config.RootFS{{Name: rootFSName, Path: rootFSPath}},
			SupportedProviders:    []string{"docker"},
			PlacementTags:         []string{"test"},
			OptionalPlacementTags: []string{"optional_tag"},
			CellID:                cellID,
			BBSAddress:            bbsURL.String(),
			ListenAddr:            fmt.Sprintf("0.0.0.0:%d", serverPort),
			ListenAddrSecurable:   fmt.Sprintf("0.0.0.0:%d", serverPortSecurable),
			LockRetryInterval:     durationjson.Duration(1 * time.Second),
			CaCertFile:            caFile,
			CertFile:              certFile,
			KeyFile:               keyFile,
			CellRegistrationsLocketEnabled: false,
			ExecutorConfig: executorinit.ExecutorConfig{
				PathToTLSCACert:              caFile,
				CachePath:                    fmt.Sprintf("%s-%d", "/tmp/cache", node),
				PathToTLSCert:                certFile,
				PathToTLSKey:                 keyFile,
				GardenAddr:                   fakeGarden.HTTPTestServer.Listener.Addr().String(),
				GardenNetwork:                "tcp",
				GardenHealthcheckProcessUser: "me",
				GardenHealthcheckProcessPath: "ls",
				ContainerMaxCpuShares:        1024,
			},
			LoggregatorConfig: diego_logging_client.Config{
				BatchFlushInterval: 10 * time.Millisecond,
				BatchMaxSize:       1,
				UseV2API:           true,
				APIPort:            metricsPort,
				CACertPath:         metronCAFile,
				KeyPath:            metronClientKeyFile,
				CertPath:           metronClientCertFile,
			},
			LagerConfig: lagerflags.LagerConfig{
				LogLevel: "debug",
			},
			ConsulCluster:     consulRunner.ConsulCluster(),
			PollingInterval:   durationjson.Duration(pollingInterval),
			EvacuationTimeout: durationjson.Duration(evacuationTimeout),
		}
	})

	JustBeforeEach(func() {
		if respondWithSuccessToCreateContainer {
			fakeGarden.RouteToHandler("POST", "/containers", ghttp.RespondWithJSONEncoded(http.StatusOK, map[string]string{"handle": "healthcheck-container"}))
		}

		runner = testrunner.New(representativePath, repConfig)
		runner.Start()
	})

	AfterEach(func(done Done) {
		close(flushEvents)
		runner.KillWithFire()
		fakeGarden.Close()
		close(done)
	})

	Context("the rep doesn't start", func() {
		BeforeEach(func() {
			fakeGarden.Start()
		})

		Context("when the metron agent isn't up", func() {
			BeforeEach(func() {
				testIngressServer.Stop()
			})

			It("logs an error and exit with non-zero status code", func() {
				Eventually(runner.Session).Should(Exit(1))
				Expect(runner.Session).To(gbytes.Say("failed-to-initialize-metron-client"))
			})
		})

		Context("when locket registration is enabled and locket address is not provided", func() {
			BeforeEach(func() {
				repConfig.CellRegistrationsLocketEnabled = true
				repConfig.LocketAddress = ""
			})

			It("logs that the locket address is required and exits non zero", func() {
				Eventually(runner.Session).Should(Exit(2))
				Expect(runner.Session).To(gbytes.Say("invalid-locket-config"))
			})
		})

		Context("when the SAN is set to localhost instead of 127.0.0.1", func() {
			BeforeEach(func() {
				caFile = path.Join(basePath, "dnssan-certs", "server-ca.crt")
				certFile = path.Join(basePath, "dnssan-certs", "server.crt")
				keyFile = path.Join(basePath, "dnssan-certs", "server.key")
				repConfig.CaCertFile = caFile
				repConfig.CertFile = certFile
				repConfig.KeyFile = keyFile

				repConfig.PathToTLSCACert = caFile
				repConfig.PathToTLSCert = certFile
				repConfig.PathToTLSKey = keyFile
			})

			It("exits with status code 2", func() {
				Eventually(runner.Session.Buffer()).Should(gbytes.Say("tls-configuration-failed"))
				Eventually(runner.Session.ExitCode).Should(Equal(2))
			})
		})

		Context("when the server cert does not have the correct SANs", func() {
			BeforeEach(func() {
				caFile = path.Join(basePath, "rouge-certs", "server-ca.crt")
				certFile = path.Join(basePath, "rouge-certs", "server.crt")
				keyFile = path.Join(basePath, "rouge-certs", "server.key")
				repConfig.PathToTLSCACert = caFile
				repConfig.PathToTLSCert = certFile
				repConfig.PathToTLSKey = keyFile
				repConfig.CaCertFile = caFile
				repConfig.CertFile = certFile
				repConfig.KeyFile = keyFile
			})

			It("exits with status code 2", func() {
				Eventually(runner.Session.Buffer()).Should(gbytes.Say("tls-configuration-failed"))
				Eventually(runner.Session.ExitCode).Should(Equal(2))
			})
		})

		Context("because an incorrect server key is supplied", func() {
			BeforeEach(func() {
				repConfig.CaCertFile = path.Join(basePath, "green-certs", "server-ca.crt")
				repConfig.CertFile = path.Join(basePath, "green-certs", "server.crt")
				repConfig.KeyFile = path.Join(basePath, "blue-certs", "server.key")
			})

			It("fails to start secure server", func() {
				Eventually(runner.Session.Buffer()).Should(gbytes.Say("failed-to-configure-secure-BBS-client"))
				Eventually(runner.Session.ExitCode).Should(Equal(2))
			})
		})

		Context("because an invalid values for certificates are supplied", func() {
			BeforeEach(func() {
				repConfig.CaCertFile = ""
				repConfig.CertFile = ""
				repConfig.KeyFile = ""
			})

			It("fails to start secure server", func() {
				Eventually(runner.Session.Buffer()).Should(gbytes.Say("failed-to-configure-secure-BBS-client"))
				Eventually(runner.Session.ExitCode).Should(Equal(2))
			})
		})
	})

	Describe("RootFS for garden healthcheck", func() {
		var createRequestReceived chan string

		BeforeEach(func() {
			fakeGarden.Start()
			respondWithSuccessToCreateContainer = false
			createRequestReceived = make(chan string)
			fakeGarden.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("POST", "/containers", ""),
					func(w http.ResponseWriter, req *http.Request) {
						body, err := ioutil.ReadAll(req.Body)
						req.Body.Close()
						Expect(err).ShouldNot(HaveOccurred())
						createRequestReceived <- string(body)
					},
				),
			)

			fakeGarden.AllowUnhandledRequests = true
		})

		It("sends the correct rootfs when creating the container", func() {
			Eventually(createRequestReceived).Should(Receive(And(
				ContainSubstring(`executor-healthcheck`),
				ContainSubstring(`"rootfs":"/path/to/rootfs"`),
			)))
		})

		Context("when there are multiple rootfses", func() {
			BeforeEach(func() {
				repConfig.PreloadedRootFS = append([]config.RootFS{{
					Name: "another",
					Path: "/path/to/another/rootfs",
				}}, repConfig.PreloadedRootFS...)
			})

			It("uses the first rootfs", func() {
				Eventually(createRequestReceived).Should(Receive(And(
					ContainSubstring(`executor-healthcheck`),
					ContainSubstring(`"rootfs":"/path/to/another/rootfs"`),
				)))
			})
		})
	})

	Context("when Garden is available", func() {
		BeforeEach(func() {
			fakeGarden.Start()
		})

		JustBeforeEach(func() {
			Eventually(runner.Session, 2).Should(gbytes.Say("rep.started"))
		})

		Context("when a value is provided caCertsForDownloads", func() {
			var certFile *os.File

			BeforeEach(func() {
				var err error
				certFile, err = ioutil.TempFile("", "")
				Expect(err).NotTo(HaveOccurred())
			})

			AfterEach(func() {
				os.Remove(certFile.Name())
			})

			Context("when the file has a valid cert bundle", func() {
				BeforeEach(func() {
					fileContents := []byte(`-----BEGIN CERTIFICATE-----
MIIBdzCCASOgAwIBAgIBADALBgkqhkiG9w0BAQUwEjEQMA4GA1UEChMHQWNtZSBD
bzAeFw03MDAxMDEwMDAwMDBaFw00OTEyMzEyMzU5NTlaMBIxEDAOBgNVBAoTB0Fj
bWUgQ28wWjALBgkqhkiG9w0BAQEDSwAwSAJBAN55NcYKZeInyTuhcCwFMhDHCmwa
IUSdtXdcbItRB/yfXGBhiex00IaLXQnSU+QZPRZWYqeTEbFSgihqi1PUDy8CAwEA
AaNoMGYwDgYDVR0PAQH/BAQDAgCkMBMGA1UdJQQMMAoGCCsGAQUFBwMBMA8GA1Ud
EwEB/wQFMAMBAf8wLgYDVR0RBCcwJYILZXhhbXBsZS5jb22HBH8AAAGHEAAAAAAA
AAAAAAAAAAAAAAEwCwYJKoZIhvcNAQEFA0EAAoQn/ytgqpiLcZu9XKbCJsJcvkgk
Se6AbGXgSlq+ZCEVo0qIwSgeBqmsJxUu7NCSOwVJLYNEBO2DtIxoYVk+MA==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIFATCCAuugAwIBAgIBATALBgkqhkiG9w0BAQswEjEQMA4GA1UEAxMHZGllZ29D
QTAeFw0xNjAyMTYyMTU1MzNaFw0yNjAyMTYyMTU1NDZaMBIxEDAOBgNVBAMTB2Rp
ZWdvQ0EwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQC7N7lGx7QGqkMd
wjqgkr09CPoV3HW+GL+YOPajf//CCo15t3mLu9Npv7O7ecb+g/6DxEOtHFpQBSbQ
igzHZkdlBJEGknwH2bsZ4wcVT2vcv2XPAIMDrnT7VuF1S2XD7BJK3n6BeXkFsVPA
OUjC/v0pM/rCFRId5CwtRD/0IHFC/qgEtFQx+zejXXEn1AJMzvNNJ3B0bd8VQGEX
ppemZXS1QvTP7/j2h7fJjosyoL6+76k4mcoScmWFNJHKcG4qcAh8rdnDlw+hJ+5S
z73CadYI2BTnlZ/fxEcsZ/kcteFSf0mFpMYX6vs9/us/rgGwjUNzg+JlzvF43TYY
VQ+TRkFUYHhDv3xwuRHnPNe0Nm30esKpqvbSXtoS6jcnpHn9tMOU0+4NW4aEdy9s
7l4lcGyih4qZfHbYTsRDk1Nrq5EzQbhlZSPC3nxMrLxXri7j22rVCY/Rj9IgAxwC
R3KcCdADGJeNOw44bK/BsRrB+Hxs9yNpXc2V2dez+w3hKNuzyk7WydC3fgXxX6x8
66xnlhFGor7fvM0OSMtGUBD16igh4ySdDiEMNUljqQ1DuMglT1eGdg+Kh+1YYWpz
v3JkNTX96C80IivbZyunZ2CczFhW2HlGWZLwNKeuM0hxt6AmiEa+KJQkx73dfg3L
tkDWWp9TXERPI/6Y2696INi0wElBUQIDAQABo2YwZDAOBgNVHQ8BAf8EBAMCAAYw
EgYDVR0TAQH/BAgwBgEB/wIBADAdBgNVHQ4EFgQU5xGtUKEzsfGmk/Siqo4fgAMs
TBwwHwYDVR0jBBgwFoAU5xGtUKEzsfGmk/Siqo4fgAMsTBwwCwYJKoZIhvcNAQEL
A4ICAQBkWgWl2t5fd4PZ1abpSQNAtsb2lfkkpxcKw+Osn9MeGpcrZjP8XoVTxtUs
GMpeVn2dUYY1sxkVgUZ0Epsgl7eZDK1jn6QfWIjltlHvDtJMh0OrxmdJUuHTGIHc
lsI9NGQRUtbyFHmy6jwIF7q925OmPQ/A6Xgkb45VUJDGNwOMUL5I9LbdBXcjmx6F
ZifEON3wxDBVMIAoS/mZYjP4zy2k1qE2FHoitwDccnCG5Wya+AHdZv/ZlfJcuMtU
U82oyHOctH29BPwASs3E1HUKof6uxJI+Y1M2kBDeuDS7DWiTt3JIVCjewIIhyYYw
uTPbQglqhqHr1RWohliDmKSroIil68s42An0fv9sUr0Btf4itKS1gTb4rNiKTZC/
8sLKs+CA5MB+F8lCllGGFfv1RFiUZBQs9+YEE+ru+yJw39lHeZQsEUgHbLjbVHs1
WFqiKTO8VKl1/eGwG0l9dI26qisIAa/I7kLjlqboKycGDmAAarsmcJBLPzS+ytiu
hoxA/fLhSWJvPXbdGemXLWQGf5DLN/8QGB63Rjp9WC3HhwSoU0NvmNmHoh+AdRRT
dYbCU/DMZjsv+Pt9flhj7ELLo+WKHyI767hJSq9A7IT3GzFt8iGiEAt1qj2yS0DX
36hwbfc1Gh/8nKgFeLmPOlBfKncjTjL2FvBNap6a8tVHXO9FvQ==
-----END CERTIFICATE-----`)

					err := ioutil.WriteFile(certFile.Name(), fileContents, os.ModePerm)
					Expect(err).NotTo(HaveOccurred())
					repConfig.PathToCACertsForDownloads = certFile.Name()
				})

				It("should start", func() {
					Consistently(runner.Session).ShouldNot(Exit())
				})
			})
		})

		Describe("when an interrupt signal is sent to the representative", func() {
			JustBeforeEach(func() {
				if runtime.GOOS == "windows" {
					Skip("Interrupt isn't supported on windows")
				}

				runner.Stop()
			})

			It("should die", func() {
				Eventually(runner.Session.ExitCode).Should(Equal(0))
			})
		})

		Context("when the bbs is down", func() {
			BeforeEach(func() {
				ginkgomon.Kill(bbsProcess)
			})

			AfterEach(func() {
				bbsRunner = bbstestrunner.New(bbsBinPath, bbsConfig)
			})

			It("starts", func() {
				Consistently(runner.Session).ShouldNot(Exit())
			})
		})

		Context("when starting", func() {
			var deleteChan chan struct{}
			BeforeEach(func() {
				fakeGarden.RouteToHandler("GET", "/containers",
					func(w http.ResponseWriter, r *http.Request) {
						r.ParseForm()
						healthcheckTagQueryParam := gardenhealth.HealthcheckTag
						if r.FormValue(healthcheckTagQueryParam) == gardenhealth.HealthcheckTagValue {
							ghttp.RespondWithJSONEncoded(http.StatusOK, struct{}{})(w, r)
						} else {
							ghttp.RespondWithJSONEncoded(http.StatusOK, map[string][]string{"handles": []string{"cnr1", "cnr2"}})(w, r)
						}
					},
				)
				deleteChan = make(chan struct{}, 2)
				fakeGarden.RouteToHandler("DELETE", "/containers/cnr1",
					ghttp.CombineHandlers(
						func(http.ResponseWriter, *http.Request) {
							deleteChan <- struct{}{}
						},
						ghttp.RespondWithJSONEncoded(http.StatusOK, &struct{}{})))
				fakeGarden.RouteToHandler("DELETE", "/containers/cnr2",
					ghttp.CombineHandlers(
						func(http.ResponseWriter, *http.Request) {
							deleteChan <- struct{}{}
						},
						ghttp.RespondWithJSONEncoded(http.StatusOK, &struct{}{})))
			})

			It("destroys any existing containers", func() {
				Eventually(deleteChan).Should(Receive())
				Eventually(deleteChan).Should(Receive())
			})
		})

		Describe("maintaining presence", func() {
			Context("with consul", func() {
				BeforeEach(func() {
					repConfig.CellRegistrationsLocketEnabled = false
				})

				It("should maintain presence", func() {
					Eventually(fetchCells(logger)).Should(HaveLen(1))

					cells, err := bbsClient.Cells(logger)
					Expect(err).NotTo(HaveOccurred())

					cellSet := models.NewCellSetFromList(cells)

					cellPresence := cellSet[cellID]
					Expect(cellPresence.CellId).To(Equal(cellID))
					Expect(cellPresence.RepAddress).To(MatchRegexp(fmt.Sprintf(`http\:\/\/.*\:%d`, serverPort)))
					Expect(cellPresence.PlacementTags).To(Equal([]string{"test"}))
					Expect(cellPresence.OptionalPlacementTags).To(Equal([]string{"optional_tag"}))
				})
			})

			Context("with locket", func() {
				var (
					locketRunner  ifrit.Runner
					locketProcess ifrit.Process
					locketAddress string
				)

				BeforeEach(func() {
					locketPort, err := portAllocator.ClaimPorts(1)
					Expect(err).NotTo(HaveOccurred())

					locketAddress = fmt.Sprintf("localhost:%d", locketPort)
					locketRunner = locketrunner.NewLocketRunner(locketBinPath, func(cfg *locketconfig.LocketConfig) {
						cfg.ConsulCluster = consulRunner.ConsulCluster()
						cfg.DatabaseConnectionString = sqlRunner.ConnectionString()
						cfg.DatabaseDriver = sqlRunner.DriverName()
						cfg.ListenAddress = locketAddress
					})
					locketProcess = ginkgomon.Invoke(locketRunner)

					repConfig.CellRegistrationsLocketEnabled = true
					repConfig.ClientLocketConfig = locketrunner.ClientLocketConfig()
					repConfig.LocketAddress = locketAddress
				})

				AfterEach(func() {
					ginkgomon.Kill(bbsProcess)
					ginkgomon.Kill(locketProcess)
				})

				It("should maintain presence", func() {
					locketClient, err := locket.NewClient(logger, repConfig.ClientLocketConfig)
					Expect(err).NotTo(HaveOccurred())

					var response *locketmodels.FetchResponse
					Eventually(func() error {
						response, err = locketClient.Fetch(context.Background(), &locketmodels.FetchRequest{Key: repConfig.CellID})
						return err
					}, 10*time.Second).Should(Succeed())
					Expect(response.Resource.Key).To(Equal(repConfig.CellID))
					Expect(response.Resource.Type).To(Equal(locketmodels.PresenceType))
					Expect(response.Resource.TypeCode).To(Equal(locketmodels.PRESENCE))
					value := &models.CellPresence{}
					err = json.Unmarshal([]byte(response.Resource.Value), value)
					Expect(err).NotTo(HaveOccurred())
					Expect(value.Zone).To(Equal(repConfig.Zone))
					Expect(value.CellId).To(Equal(repConfig.CellID))
				})

				Context("when it loses its presence", func() {
					var locketClient locketmodels.LocketClient

					JustBeforeEach(func() {
						var err error
						locketClient, err = locket.NewClient(logger, repConfig.ClientLocketConfig)
						Expect(err).NotTo(HaveOccurred())

						Eventually(func() error {
							_, err = locketClient.Fetch(context.Background(), &locketmodels.FetchRequest{Key: repConfig.CellID})
							return err
						}, 10*time.Second).Should(Succeed())
					})

					It("does not exit", func() {
						ginkgomon.Kill(locketProcess)
						Consistently(runner.Session, locket.RetryInterval).ShouldNot(Exit())
					})
				})
			})
		})

		Context("acting as an auction representative", func() {
			var repClient rep.Client

			JustBeforeEach(func() {
				Eventually(fetchCells(logger)).Should(HaveLen(1))
				cells, err := bbsClient.Cells(logger)
				cellSet := models.NewCellSetFromList(cells)
				Expect(err).NotTo(HaveOccurred())

				tlsConfig := &rep.TLSConfig{
					RequireTLS: true,
					CertFile:   certFile,
					KeyFile:    keyFile,
					CaCertFile: caFile,
				}
				factory, err := rep.NewClientFactory(cfhttp.NewClient(), cfhttp.NewCustomTimeoutClient(time.Second), tlsConfig)

				Expect(err).NotTo(HaveOccurred())
				u, err := url.Parse(cellSet[cellID].RepUrl)
				Expect(err).NotTo(HaveOccurred())
				_, p, err := net.SplitHostPort(u.Host)
				Expect(err).NotTo(HaveOccurred())
				u.Host = net.JoinHostPort("127.0.0.1", p) // no dns lookup in unit tests
				repClient, err = factory.CreateClient("", u.String())
				Expect(err).NotTo(HaveOccurred())
			})

			Context("State", func() {
				It("returns the cell id and rep url in the state info", func() {
					state, err := repClient.State(logger)
					Expect(err).NotTo(HaveOccurred())
					Expect(state.CellID).To(Equal(cellID))
					url := fmt.Sprintf("https://%s.cell.service.cf.internal:%d", cellID, serverPortSecurable)
					url = strings.Replace(url, "_", "-", -1)
					Expect(state.RepURL).To(Equal(url))
				})

				Context("when an lrp is requested", func() {
					var (
						placementTags, volumeDrivers []string
					)

					BeforeEach(func() {
						placementTags = []string{"pg-1"}
						volumeDrivers = []string{"vd-1"}
					})

					JustBeforeEach(func() {
						work := rep.Work{
							LRPs: []rep.LRP{
								rep.NewLRP(
									"",
									models.NewActualLRPKey("pg-1", 0, "domain"),
									rep.NewResource(100, 100, 10),
									rep.NewPlacementConstraint("foobar", placementTags, volumeDrivers),
								),
							},
						}
						failed, err := repClient.Perform(logger, work)
						Expect(err).NotTo(HaveOccurred())
						Expect(failed.LRPs).To(HaveLen(0))
					})

					It("returns the lrp info ", func() {
						state, err := repClient.State(logger)
						Expect(err).NotTo(HaveOccurred())
						Expect(state.LRPs).To(HaveLen(1))
						lrp := state.LRPs[0]
						Expect(lrp.InstanceGUID).NotTo(BeEmpty())
						Expect(lrp.PlacementConstraint.PlacementTags).To(Equal(placementTags))
						Expect(lrp.PlacementConstraint.VolumeDrivers).To(Equal(volumeDrivers))
						Expect(lrp.RootFs).To(Equal("foobar"))
						Expect(lrp.MaxPids).To(BeEquivalentTo(10))
						Expect(lrp.State).To(BeEquivalentTo("CLAIMED"))
					})
				})
			})

			Context("Capacity with a container", func() {
				It("returns total capacity and state information", func() {
					state, err := repClient.State(logger)
					Expect(err).NotTo(HaveOccurred())
					Expect(state.TotalResources).To(Equal(rep.Resources{
						MemoryMB:   1024,
						DiskMB:     10 * 1024,
						Containers: 3,
					}))
					Expect(state.PlacementTags).To(Equal([]string{"test"}))
					Expect(state.OptionalPlacementTags).To(Equal([]string{"optional_tag"}))
				})

				Context("when the container is removed", func() {
					It("returns available capacity == total capacity", func() {
						fakeGarden.RouteToHandler("GET", "/containers", ghttp.RespondWithJSONEncoded(http.StatusOK, struct{}{}))
						fakeGarden.RouteToHandler("GET", "/containers/bulk_info", ghttp.RespondWithJSONEncoded(http.StatusOK, struct{}{}))

						Eventually(func() rep.Resources {
							state, err := repClient.State(logger)
							Expect(err).NotTo(HaveOccurred())
							return state.AvailableResources
						}).Should(Equal(rep.Resources{
							MemoryMB:   1024,
							DiskMB:     10 * 1024,
							Containers: 3,
						}))
					})
				})
			})
		})

		Describe("polling the BBS for tasks to reap", func() {
			var task *models.Task

			JustBeforeEach(func() {
				task = model_helpers.NewValidTask("task-guid")
				err := bbsClient.DesireTask(logger, task.TaskGuid, task.Domain, task.TaskDefinition)
				Expect(err).NotTo(HaveOccurred())

				_, err = bbsClient.StartTask(logger, task.TaskGuid, cellID)
				Expect(err).NotTo(HaveOccurred())
			})

			It("eventually marks tasks with no corresponding container as failed", func() {
				Eventually(func() []*models.Task {
					return getTasksByState(logger, bbsClient, models.Task_Completed)
				}, 5*pollingInterval).Should(HaveLen(1))

				completedTasks := getTasksByState(logger, bbsClient, models.Task_Completed)

				Expect(completedTasks[0].TaskGuid).To(Equal(task.TaskGuid))
				Expect(completedTasks[0].Failed).To(BeTrue())
			})
		})

		Describe("polling the BBS for actual LRPs to reap", func() {
			JustBeforeEach(func() {
				desiredLRP := &models.DesiredLRP{
					ProcessGuid: "process-guid",
					RootFs:      "some:rootfs",
					Domain:      "some-domain",
					Instances:   1,
					Action: models.WrapAction(&models.RunAction{
						User: "me",
						Path: "the-path",
						Args: []string{},
					}),
				}
				actualLRPKey := models.ActualLRPKey{
					ProcessGuid: desiredLRP.ProcessGuid,
					Index:       0,
				}

				err := bbsClient.DesireLRP(logger, desiredLRP)
				Expect(err).NotTo(HaveOccurred())

				instanceKey := models.NewActualLRPInstanceKey("some-instance-guid", cellID)

				err = bbsClient.ClaimActualLRP(logger, &actualLRPKey, &instanceKey)
				Expect(err).NotTo(HaveOccurred())
			})

			It("eventually reaps actual LRPs with no corresponding container", func() {
				Eventually(getActualLRPGroups(logger), 5*pollingInterval).Should(BeEmpty())
			})
		})

		Describe("Evacuation", func() {
			JustBeforeEach(func() {
				resp, err := client.Post(fmt.Sprintf("https://127.0.0.1:%d/evacuate", serverPort), "text/html", nil)
				Expect(err).NotTo(HaveOccurred())
				resp.Body.Close()
				Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
			})

			Context("when exceeding the evacuation timeout", func() {
				It("shuts down gracefully", func() {
					// wait longer than expected to let OS and Go runtime reap process
					Eventually(runner.Session.ExitCode, 2*evacuationTimeout+2*time.Second).Should(Equal(0))
				})
			})

			Context("when signaled to stop", func() {
				JustBeforeEach(func() {
					runner.Stop()
				})

				It("shuts down gracefully", func() {
					Eventually(runner.Session.ExitCode).Should(Equal(0))
				})
			})
		})

		Describe("when a Ping request comes in", func() {
			It("responds with 200 OK", func() {
				resp, err := client.Get(fmt.Sprintf("https://127.0.0.1:%d/ping", serverPort))
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(http.StatusOK))
			})
		})

		Describe("ServiceRegistration", func() {
			Context("when consul service registration is enabled", func() {
				BeforeEach(func() {
					repConfig.EnableConsulServiceRegistration = true
				})

				It("registers itself with consul", func() {
					consulClient := consulRunner.NewClient()
					services, err := consulClient.Agent().Services()
					Expect(err).ToNot(HaveOccurred())

					Expect(services).To(HaveKeyWithValue("cell",
						&api.AgentService{
							Service: "cell",
							ID:      "cell",
							Port:    int(serverPort),
							Tags:    []string{strings.Replace(cellID, "_", "-", -1)},
						}))
				})

				It("registers a TTL healthcheck", func() {
					consulClient := consulRunner.NewClient()
					checks, err := consulClient.Agent().Checks()
					Expect(err).ToNot(HaveOccurred())

					Expect(checks).To(HaveKeyWithValue("service:cell",
						&api.AgentCheck{
							Node:        "0",
							CheckID:     "service:cell",
							Name:        "Service 'cell' check",
							Status:      "passing",
							ServiceID:   "cell",
							ServiceName: "cell",
						}))
				})
			})

			Context("when consul service registration is disabled", func() {
				It("does not register itself with consul", func() {
					services, err := consulRunner.NewClient().Agent().Services()
					Expect(err).ToNot(HaveOccurred())

					Expect(services).NotTo(HaveKey("cell"))
				})
			})
		})

		It("serves the localhost-only endpoints over TLS", func() {
			resp, err := client.Get(fmt.Sprintf("https://127.0.0.1:%d/ping", serverPort))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
		})

		It("has only insecure routes on the locally accessible server", func() {
			resp, err := client.Get(fmt.Sprintf("https://127.0.0.1:%d/ping", serverPort))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			resp, err = client.Get(fmt.Sprintf("https://127.0.0.1:%d/state", serverPort))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusNotFound))
		})

		Context("for the network accessible https server", func() {
			It("does not have unsecured routes", func() {
				resp, err := client.Get(fmt.Sprintf("https://127.0.0.1:%d/ping", serverPortSecurable))
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(http.StatusNotFound))
			})

			It("has secured routes", func() {
				resp, err := client.Get(fmt.Sprintf("https://127.0.0.1:%d/state", serverPortSecurable))
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(http.StatusOK))
			})
		})

		Context("ClientFactory", func() {
			var (
				addr          string
				tlsConfig     *rep.TLSConfig
				client        *http.Client
				clientFactory rep.ClientFactory
			)

			BeforeEach(func() {
				caFile = path.Join(basePath, "green-certs", "server-ca.crt")
				certFile = path.Join(basePath, "green-certs", "server.crt")
				keyFile = path.Join(basePath, "green-certs", "server.key")
			})

			JustBeforeEach(func() {
				addr = fmt.Sprintf("https://127.0.0.1:%d", serverPortSecurable)
				client = cfhttp.NewClient()
				var err error
				clientFactory, err = rep.NewClientFactory(client, client, tlsConfig)
				Expect(err).NotTo(HaveOccurred())
			})

			canConnectSuccessfully := func() {
				client, err := clientFactory.CreateClient("", addr)
				Expect(err).NotTo(HaveOccurred())
				_, err = client.State(logger)
				Expect(err).NotTo(HaveOccurred())
			}

			Context("doesn't support tls", func() {
				BeforeEach(func() {
					tlsConfig = nil
				})

				It("cannot create a new client using the address", func() {
					_, err := clientFactory.CreateClient("", addr)
					Expect(err).To(MatchError(ContainSubstring("https scheme not supported")))
				})
			})

			Context("prefers tls", func() {
				BeforeEach(func() {
					tlsConfig = &rep.TLSConfig{
						RequireTLS: false,
						CaCertFile: caFile,
						KeyFile:    keyFile,
						CertFile:   certFile,
					}
				})

				It("can connect to the secure url", func() {
					canConnectSuccessfully()
				})
			})

			Context("requires tls", func() {
				BeforeEach(func() {
					tlsConfig = &rep.TLSConfig{
						RequireTLS: true,
						CaCertFile: caFile,
						KeyFile:    keyFile,
						CertFile:   certFile,
					}
				})

				It("can connect to the secure url", func() {
					canConnectSuccessfully()
				})

				It("sets a session cache", func() {
					// make sure the client factory change the transport setting of the
					// client passed in
					tlsConfig := client.Transport.(*http.Transport).TLSClientConfig
					Expect(tlsConfig).NotTo(BeNil())
					Expect(tlsConfig.ClientSessionCache).NotTo(BeNil())
				})
			})
		})
	})

	Context("when Garden is unavailable", func() {
		It("should not exit and continue waiting for a connection", func() {
			Consistently(runner.Session.Buffer()).ShouldNot(gbytes.Say("started"))
			Consistently(runner.Session).ShouldNot(Exit())
		})

		It("ping should fail", func() {
			Consistently(func() bool {
				resp, err := client.Get(fmt.Sprintf("https://127.0.0.1:%d/ping", serverPort))
				if err != nil {
					return true
				}
				return resp.StatusCode != http.StatusOK
			}, 5*time.Second).Should(BeTrue())
		})

		Context("when Garden starts", func() {
			JustBeforeEach(func() {
				fakeGarden.Start()
				// these tests only look for the start of a sequence of requests
				fakeGarden.AllowUnhandledRequests = false
			})

			It("should connect", func() {
				Eventually(runner.Session.Buffer(), 5*time.Second).Should(gbytes.Say("started"))
			})

			Context("while the healthcheck container is being created", func() {
				var (
					blockCh chan struct{}
				)

				BeforeEach(func() {
					blockCh = make(chan struct{})
				})

				AfterEach(func() {
					close(blockCh)
				})

				JustBeforeEach(func() {
					// TODO: block the create call
					fakeGarden.RouteToHandler("POST", "/containers/healthcheck-container/processes", http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
						<-blockCh
					}))
				})

				It("ping should fail", func() {
					Consistently(func() bool {
						resp, err := client.Get(fmt.Sprintf("https://127.0.0.1:%d/ping", serverPort))
						if err != nil {
							return true
						}
						return resp.StatusCode != http.StatusOK
					}, 5*time.Second).Should(BeTrue())
				})
			})

			It("ping should succeed", func() {
				Eventually(func() bool {
					resp, err := client.Get(fmt.Sprintf("https://127.0.0.1:%d/ping", serverPort))
					if err != nil {
						return true
					}
					return resp.StatusCode != http.StatusOK
				}, 5*time.Second).Should(BeFalse())
			})
		})
	})
})

func getTasksByState(logger lager.Logger, client bbs.InternalClient, state models.Task_State) []*models.Task {
	tasks, err := client.Tasks(logger)
	Expect(err).NotTo(HaveOccurred())

	filteredTasks := make([]*models.Task, 0)
	for _, task := range tasks {
		if task.State == state {
			filteredTasks = append(filteredTasks, task)
		}
	}
	return filteredTasks
}

func fetchCells(logger lager.Logger) func() ([]*models.CellPresence, error) {
	return func() ([]*models.CellPresence, error) {
		return bbsClient.Cells(logger)
	}
}
