package config_test

import (
	"io/ioutil"
	"os"
	"time"

	"code.cloudfoundry.org/debugserver"
	"code.cloudfoundry.org/executor/initializer/configuration"
	"code.cloudfoundry.org/lager/lagerflags"
	"code.cloudfoundry.org/rep/cmd/rep/config"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RepConfig", func() {
	var configFilePath, configData string

	BeforeEach(func() {
		configData = `{
			"log_level": "debug",
			"debug_address": "5.5.5.5:9090",
			"garden_network": "test-network",
			"garden_addr": "100.0.0.1",
			"container_owner_name": "vcap",
			"healthcheck_container_owner_name": "vcap_health",
			"temp_dir": "/tmp/test",
			"cache_path": "/tmp/cache",
			"max_cache_in_bytes": 101,
			"skip_cert_verify": true,
			"path_to_ca_certs_for_downloads": "/tmp/ca-certs",
			"export_network_env_vars": false,
			"volman_driver_paths": "/tmp/volman1:/tmp/volman2",
			"container_max_cpu_shares": 4,
			"container_inode_limit": 1000,
			"healthy_monitoring_interval": "5s",
			"unhealthy_monitoring_interval": "10s",
			"healthcheck_work_pool_size": 10,
			"max_concurrent_downloads": 11,
			"create_work_pool_size": 15,
			"delete_work_pool_size": 10,
			"read_work_pool_size": 15,
			"metrics_work_pool_size": 5,
			"reserved_expiration_time": "10s",
			"container_reap_interval": "11s",
			"garden_healthcheck_rootfs": "test-rootfs",
			"healthy_monitoring_interval": "5s",
			"garden_healthcheck_interval": "12s",
			"garden_healthcheck_emission_interval": "13s",
			"garden_healthcheck_timeout": "14s",
			"garden_healthcheck_command_retry_pause": "15s",
			"garden_healthcheck_process_path": "/tmp/healthcheck-process",
			"garden_healthcheck_process_user": "vcap_health",
			"garden_healthcheck_process_dir": "/tmp/process",
			"memory_mb": "1000",
			"disk_mb": "20000",
			"post_setup_hook": "post_setup_hook",
			"post_setup_user": "post_setup_user",
			"trusted_system_certificates_path": "/tmp/trusted",
			"container_metrics_report_interval": "16s",
			"session_name": "test",
			"consul_cluster": "test cluster",
			"lock_ttl": "5s",
			"lock_retry_interval": "5s",
      "listen_addr": "0.0.0.0:8080",
			"listen_addr_securable": "0.0.0.0:8081",
			"listen_addr_admin": "0.0.0.1:8081",
			"require_tls": true,
			"ca_cert": "/tmp/ca_cert",
			"server_cert": "/tmp/server_cert",
			"server_key": "/tmp/server_key",
			"cell_id" : "cell_z1/10",
			"zone": "test-zone",
			"polling_interval": "10s",
			"dropsonde_port": 8082,
			"communication_timeout": "11s",
			"evacuation_timeout" : "12s",
			"evacuation_polling_interval" : "13s",
			"bbs_api_url": "1.1.1.1:9091",
			"advertise_domain": "test-domain",
			"enable_legacy_api_endpoints": true,
			"bbs_ca_cert": "/tmp/bbs_ca_cert",
			"bbs_client_cert": "/tmp/bbs_client_cert",
			"bbs_client_key": "/tmp/bbs_client_key",
			"bbs_client_session_cache_size": 100,
			"bbs_max_idle_conns_per_host": 10,
      "preloaded_root_fs": ["test:value", "test2:value2"],
      "supported_providers": ["provider1", "provider2"],
      "garden_healthcheck_process_env": ["env1", "env2"],
      "garden_healthcheck_process_args": ["arg1", "arg2"],
      "placement_tags": ["tag1", "tag2"],
      "optional_placement_tags": ["otag1", "otag2"]
		}`
	})

	JustBeforeEach(func() {
		configFile, err := ioutil.TempFile("", "config-file")
		Expect(err).NotTo(HaveOccurred())

		n, err := configFile.WriteString(configData)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(len(configData)))

		configFilePath = configFile.Name()
	})

	AfterEach(func() {
		err := os.RemoveAll(configFilePath)
		Expect(err).NotTo(HaveOccurred())
	})

	It("correctly parses the config file", func() {
		repConfig, err := config.NewRepConfig(configFilePath)
		Expect(err).NotTo(HaveOccurred())

		Expect(repConfig).To(Equal(config.RepConfig{
			LagerConfig: lagerflags.LagerConfig{
				LogLevel: lagerflags.DEBUG,
			},
			DebugServerConfig: debugserver.DebugServerConfig{
				DebugAddress: "5.5.5.5:9090",
			},
			ExecutorConfig: config.ExecutorConfig{
				GardenNetwork:                 "test-network",
				GardenAddr:                    "100.0.0.1",
				ContainerOwnerName:            "vcap",
				HealthCheckContainerOwnerName: "vcap_health",
				TempDir:                            "/tmp/test",
				CachePath:                          "/tmp/cache",
				MaxCacheSizeInBytes:                101,
				SkipCertVerify:                     true,
				PathToCACertsForDownloads:          "/tmp/ca-certs",
				ExportNetworkEnvVars:               false,
				VolmanDriverPaths:                  "/tmp/volman1:/tmp/volman2",
				ContainerMaxCpuShares:              4,
				ContainerInodeLimit:                1000,
				HealthyMonitoringInterval:          5000000000,
				UnhealthyMonitoringInterval:        10000000000,
				HealthCheckWorkPoolSize:            10,
				MaxConcurrentDownloads:             11,
				CreateWorkPoolSize:                 15,
				DeleteWorkPoolSize:                 10,
				ReadWorkPoolSize:                   15,
				MetricsWorkPoolSize:                5,
				ReservedExpirationTime:             10000000000,
				ContainerReapInterval:              11000000000,
				GardenHealthcheckInterval:          12000000000,
				GardenHealthcheckEmissionInterval:  13000000000,
				GardenHealthcheckTimeout:           14000000000,
				GardenHealthcheckCommandRetryPause: 15000000000,
				GardenHealthcheckProcessPath:       "/tmp/healthcheck-process",
				GardenHealthcheckProcessUser:       "vcap_health",
				GardenHealthcheckProcessDir:        "/tmp/process",
				GardenHealthcheckProcessEnv:        []string{"env1", "env2"},
				GardenHealthcheckProcessArgs:       []string{"arg1", "arg2"},
				MemoryMB:                           "1000",
				DiskMB:                             "20000",
				PostSetupHook:                      "post_setup_hook",
				PostSetupUser:                      "post_setup_user",
				TrustedSystemCertificatesPath:      "/tmp/trusted",
				ContainerMetricsReportInterval:     16000000000,
			},
			SessionName:               "test",
			ConsulCluster:             "test cluster",
			LockTTL:                   config.Duration(5 * time.Second),
			LockRetryInterval:         config.Duration(5 * time.Second),
			ListenAddr:                "0.0.0.0:8080",
			ListenAddrSecurable:       "0.0.0.0:8081",
			ListenAddrAdmin:           "0.0.0.1:8081",
			RequireTLS:                true,
			CaCert:                    "/tmp/ca_cert",
			ServerCert:                "/tmp/server_cert",
			ServerKey:                 "/tmp/server_key",
			CellID:                    "cell_z1/10",
			Zone:                      "test-zone",
			Pollinginterval:           config.Duration(10 * time.Second),
			DropsondePort:             8082,
			CommunicationTimeout:      config.Duration(11 * time.Second),
			EvacuationTimeout:         config.Duration(12 * time.Second),
			EvacuationPollinginterval: config.Duration(13 * time.Second),
			BbsAddress:                "1.1.1.1:9091",
			AdvertiseDomain:           "test-domain",
			EnableLegacyApiServer:     true,
			BbsCACert:                 "/tmp/bbs_ca_cert",
			BbsClientCert:             "/tmp/bbs_client_cert",
			BbsClientKey:              "/tmp/bbs_client_key",
			BbsClientSessionCacheSize: 100,
			BbsMaxIdleConnsPerHost:    10,
			PreloadedRootFS:           map[string]string{"test": "value", "test2": "value2"},
			SupportedProviders:        []string{"provider1", "provider2"},
			PlacementTags:             []string{"tag1", "tag2"},
			OptionalPlacementTags:     []string{"otag1", "otag2"},
		}))
	})

	Context("when the file does not exist", func() {
		It("returns an error", func() {
			_, err := config.NewRepConfig("foobar")
			Expect(err).To(HaveOccurred())
		})
	})

	Context("when the file does not contain valid json", func() {
		BeforeEach(func() {
			configData = "{{"
		})

		It("returns an error", func() {
			_, err := config.NewRepConfig(configFilePath)
			Expect(err).To(HaveOccurred())
		})

		Context("because the communication_timeout is not valid", func() {
			BeforeEach(func() {
				configData = `{"communication_timeout": 4234342342}`
			})

			It("returns an error", func() {
				_, err := config.NewRepConfig(configFilePath)
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Context("default values", func() {
		BeforeEach(func() {
			configData = `{}`
		})

		It("uses default values when they are not specified", func() {
			repConfig, err := config.NewRepConfig(configFilePath)
			Expect(err).NotTo(HaveOccurred())

			Expect(repConfig).To(Equal(config.RepConfig{
				ListenAddr:                "0.0.0.0:1800",
				ListenAddrSecurable:       "0.0.0.0:1801",
				RequireTLS:                true,
				Pollinginterval:           config.Duration(30 * time.Second),
				DropsondePort:             3457,
				CommunicationTimeout:      config.Duration(10 * time.Second),
				EvacuationPollinginterval: config.Duration(10 * time.Second),
				AdvertiseDomain:           "cell.service.cf.internal",
				EnableLegacyApiServer:     true,
				BbsClientSessionCacheSize: 0,
				EvacuationTimeout:         config.Duration(10 * time.Minute),
				LagerConfig:               lagerflags.DefaultLagerConfig(),
				ExecutorConfig: config.ExecutorConfig{
					GardenNetwork:                      "unix",
					GardenAddr:                         "/tmp/garden.sock",
					MemoryMB:                           configuration.Automatic,
					DiskMB:                             configuration.Automatic,
					TempDir:                            "/tmp",
					ReservedExpirationTime:             config.Duration(time.Minute),
					ContainerReapInterval:              config.Duration(time.Minute),
					ContainerInodeLimit:                200000,
					ContainerMaxCpuShares:              0,
					CachePath:                          "/tmp/cache",
					MaxCacheSizeInBytes:                10 * 1024 * 1024 * 1024,
					SkipCertVerify:                     false,
					HealthyMonitoringInterval:          config.Duration(30 * time.Second),
					UnhealthyMonitoringInterval:        config.Duration(500 * time.Millisecond),
					ExportNetworkEnvVars:               false,
					ContainerOwnerName:                 "executor",
					HealthCheckContainerOwnerName:      "executor-health-check",
					CreateWorkPoolSize:                 32,
					DeleteWorkPoolSize:                 32,
					ReadWorkPoolSize:                   64,
					MetricsWorkPoolSize:                8,
					HealthCheckWorkPoolSize:            64,
					MaxConcurrentDownloads:             5,
					GardenHealthcheckInterval:          config.Duration(10 * time.Minute),
					GardenHealthcheckEmissionInterval:  config.Duration(30 * time.Second),
					GardenHealthcheckTimeout:           config.Duration(10 * time.Minute),
					GardenHealthcheckCommandRetryPause: config.Duration(1 * time.Second),
					GardenHealthcheckProcessArgs:       []string{},
					GardenHealthcheckProcessEnv:        []string{},
					ContainerMetricsReportInterval:     config.Duration(15 * time.Second),
				},
			}))
		})
	})
})
