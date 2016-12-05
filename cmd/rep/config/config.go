package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"code.cloudfoundry.org/debugserver"
	"code.cloudfoundry.org/executor/initializer/configuration"
	"code.cloudfoundry.org/lager/lagerflags"
	"code.cloudfoundry.org/locket"
)

type StackMap map[string]string

func (m *StackMap) UnmarshalJSON(data []byte) error {
	*m = make(map[string]string)
	arr := []string{}
	err := json.Unmarshal(data, &arr)
	if err != nil {
		return err
	}

	for _, s := range arr {
		parts := strings.SplitN(s, ":", 2)
		if len(parts) != 2 {
			return errors.New("Invalid preloaded RootFS value: not of the form 'stack-name:path'")
		}

		if parts[0] == "" {
			return errors.New("Invalid preloaded RootFS value: blank stack")
		}

		if parts[1] == "" {
			return errors.New("Invalid preloaded RootFS value: blank path")
		}

		(*m)[parts[0]] = parts[1]
	}

	return nil
}

func (m StackMap) MarshalJSON() (b []byte, err error) {
	arr := []string{}
	for k, v := range m {
		arr = append(arr, fmt.Sprintf("%s:%s", k, v))
	}
	data, err := json.Marshal(arr)
	if err != nil {
		return nil, err
	}
	return data, nil
}

type Duration time.Duration

func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}

	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}

	*d = Duration(dur)
	return nil
}

func (d Duration) MarshalJSON() (b []byte, err error) {
	t := time.Duration(d)
	return []byte(fmt.Sprintf(`"%s"`, t.String())), nil
}

type RepConfig struct {
	lagerflags.LagerConfig
	debugserver.DebugServerConfig
	ExecutorConfig
	SessionName               string   `json:"session_name"`
	ConsulCluster             string   `json:"consul_cluster"`
	LockTTL                   Duration `json:"lock_ttl"`
	LockRetryInterval         Duration `json:"lock_retry_interval"`
	ListenAddr                string   `json:"listen_addr,omitempty"`
	ListenAddrSecurable       string   `json:"listen_addr_securable,omitempty"`
	ListenAddrAdmin           string   `json:"listen_addr_admin"`
	RequireTLS                bool     `json:"require_tls"`
	CaCert                    string   `json:"ca_cert"`
	ServerCert                string   `json:"server_cert"`
	ServerKey                 string   `json:"server_key"`
	CellID                    string   `json:"cell_id"`
	Zone                      string   `json:"zone"`
	PollingInterval           Duration `json:"polling_interval,omitempty"`
	DropsondePort             int      `json:"dropsonde_port,omitempty"`
	CommunicationTimeout      Duration `json:"communication_timeout,omitempty"`
	EvacuationTimeout         Duration `json:"evacuation_timeout,omitempty"`
	EvacuationPollingInterval Duration `json:"evacuation_polling_interval,omitempty"`
	BBSAddress                string   `json:"bbs_api_url"`
	AdvertiseDomain           string   `json:"advertise_domain"`
	EnableLegacyAPIServer     bool     `json:"enable_legacy_api_endpoints,omitempty"`
	BBSCACert                 string   `json:"bbs_ca_cert"`
	BBSClientCert             string   `json:"bbs_client_cert"`
	BBSClientKey              string   `json:"bbs_client_key"`
	BBSClientSessionCacheSize int      `json:"bbs_client_session_cache_size,omitempty"`
	BBSMaxIdleConnsPerHost    int      `json:"bbs_max_idle_conns_per_host"`
	PreloadedRootFS           StackMap `json:"preloaded_root_fs"`
	SupportedProviders        []string `json:"supported_providers"`
	PlacementTags             []string `json:"placement_tags"`
	OptionalPlacementTags     []string `json:"optional_placement_tags"`
}

type ExecutorConfig struct {
	GardenNetwork                      string   `json:"garden_network,omitempty"`
	GardenAddr                         string   `json:"garden_addr,omitempty"`
	ContainerOwnerName                 string   `json:"container_owner_name,omitempty"`
	HealthCheckContainerOwnerName      string   `json:"healthcheck_container_owner_name,omitempty"`
	TempDir                            string   `json:"temp_dir,omitempty"`
	CachePath                          string   `json:"cache_path,omitempty"`
	MaxCacheSizeInBytes                uint64   `json:"max_cache_in_bytes,omitempty"`
	SkipCertVerify                     bool     `json:"skip_cert_verify,omitempty"`
	PathToCACertsForDownloads          string   `json:"path_to_ca_certs_for_downloads,omitempty"`
	ExportNetworkEnvVars               bool     `json:"export_network_env_vars,omitempty"`
	VolmanDriverPaths                  string   `json:"volman_driver_paths"`
	ContainerMaxCpuShares              uint64   `json:"container_max_cpu_shares,omitempty"`
	ContainerInodeLimit                uint64   `json:"container_inode_limit,omitempty"`
	HealthyMonitoringInterval          Duration `json:"healthy_monitoring_interval,omitempty"`
	UnhealthyMonitoringInterval        Duration `json:"unhealthy_monitoring_interval,omitempty"`
	HealthCheckWorkPoolSize            int      `json:"healthcheck_work_pool_size,omitempty"`
	MaxConcurrentDownloads             int      `json:"max_concurrent_downloads,omitempty"`
	CreateWorkPoolSize                 int      `json:"create_work_pool_size,omitempty"`
	DeleteWorkPoolSize                 int      `json:"delete_work_pool_size,omitempty"`
	ReadWorkPoolSize                   int      `json:"read_work_pool_size,omitempty"`
	MetricsWorkPoolSize                int      `json:"metrics_work_pool_size,omitempty"`
	ReservedExpirationTime             Duration `json:"reserved_expiration_time,omitempty"`
	ContainerReapInterval              Duration `json:"container_reap_interval,omitempty"`
	GardenHealthcheckInterval          Duration `json:"garden_healthcheck_interval,omitempty"`
	GardenHealthcheckEmissionInterval  Duration `json:"garden_healthcheck_emission_interval,omitempty"`
	GardenHealthcheckTimeout           Duration `json:"garden_healthcheck_timeout,omitempty"`
	GardenHealthcheckCommandRetryPause Duration `json:"garden_healthcheck_command_retry_pause,omitempty"`
	GardenHealthcheckProcessPath       string   `json:"garden_healthcheck_process_path,omitempty"`
	GardenHealthcheckProcessUser       string   `json:"garden_healthcheck_process_user,omitempty"`
	GardenHealthcheckProcessDir        string   `json:"garden_healthcheck_process_dir,omitempty"`
	GardenHealthcheckProcessEnv        []string `json:"garden_healthcheck_process_env,omitempty"`
	GardenHealthcheckProcessArgs       []string `json:"garden_healthcheck_process_args,omitempty"`
	MemoryMB                           string   `json:"memory_mb,omitempty"`
	DiskMB                             string   `json:"disk_mb,omitempty"`
	PostSetupHook                      string   `json:"post_setup_hook"`
	PostSetupUser                      string   `json:"post_setup_user"`
	TrustedSystemCertificatesPath      string   `json:"trusted_system_certificates_path"`
	ContainerMetricsReportInterval     Duration `json:"container_metrics_report_interval,omitempty"`
}

func defaultConfig() RepConfig {
	return RepConfig{
		LockTTL:                   Duration(locket.DefaultSessionTTL),
		LockRetryInterval:         Duration(locket.RetryInterval),
		ListenAddr:                "0.0.0.0:1800",
		ListenAddrSecurable:       "0.0.0.0:1801",
		RequireTLS:                true,
		PollingInterval:           Duration(30 * time.Second),
		DropsondePort:             3457,
		CommunicationTimeout:      Duration(10 * time.Second),
		EvacuationPollingInterval: Duration(10 * time.Second),
		AdvertiseDomain:           "cell.service.cf.internal",
		EnableLegacyAPIServer:     true,
		BBSClientSessionCacheSize: 0,
		EvacuationTimeout:         Duration(10 * time.Minute),
		LagerConfig:               lagerflags.DefaultLagerConfig(),
		ExecutorConfig: ExecutorConfig{
			GardenNetwork:                      "unix",
			GardenAddr:                         "/tmp/garden.sock",
			MemoryMB:                           configuration.Automatic,
			DiskMB:                             configuration.Automatic,
			TempDir:                            "/tmp",
			ReservedExpirationTime:             Duration(time.Minute),
			ContainerReapInterval:              Duration(time.Minute),
			ContainerInodeLimit:                200000,
			ContainerMaxCpuShares:              0,
			CachePath:                          "/tmp/cache",
			MaxCacheSizeInBytes:                10 * 1024 * 1024 * 1024,
			SkipCertVerify:                     false,
			HealthyMonitoringInterval:          Duration(30 * time.Second),
			UnhealthyMonitoringInterval:        Duration(500 * time.Millisecond),
			ExportNetworkEnvVars:               false,
			ContainerOwnerName:                 "executor",
			HealthCheckContainerOwnerName:      "executor-health-check",
			CreateWorkPoolSize:                 32,
			DeleteWorkPoolSize:                 32,
			ReadWorkPoolSize:                   64,
			MetricsWorkPoolSize:                8,
			HealthCheckWorkPoolSize:            64,
			MaxConcurrentDownloads:             5,
			GardenHealthcheckInterval:          Duration(10 * time.Minute),
			GardenHealthcheckEmissionInterval:  Duration(30 * time.Second),
			GardenHealthcheckTimeout:           Duration(10 * time.Minute),
			GardenHealthcheckCommandRetryPause: Duration(1 * time.Second),
			GardenHealthcheckProcessArgs:       []string{},
			GardenHealthcheckProcessEnv:        []string{},
			ContainerMetricsReportInterval:     Duration(15 * time.Second),
		},
	}
}

func NewRepConfig(configPath string) (RepConfig, error) {
	repConfig := defaultConfig()
	configFile, err := os.Open(configPath)
	if err != nil {
		return RepConfig{}, err
	}
	decoder := json.NewDecoder(configFile)

	err = decoder.Decode(&repConfig)
	if err != nil {
		return RepConfig{}, err
	}

	return repConfig, nil
}
