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
		fmt.Printf("data: %#v", data)
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
	AdvertiseDomain           string   `json:"advertise_domain,omitempty"`
	BBSAddress                string   `json:"bbs_api_url"`
	BBSCACertFile             string   `json:"bbs_ca_cert_file"`
	BBSClientCertFile         string   `json:"bbs_client_cert_file"`
	BBSClientKeyFile          string   `json:"bbs_client_key_file"`
	BBSClientSessionCacheSize int      `json:"bbs_client_session_cache_size,omitempty"`
	BBSMaxIdleConnsPerHost    int      `json:"bbs_max_idle_conns_per_host,omitempty"`
	CaCertFile                string   `json:"ca_cert_file"`
	CellID                    string   `json:"cell_id"`
	CommunicationTimeout      Duration `json:"communication_timeout,omitempty"`
	ConsulCluster             string   `json:"consul_cluster"`
	DropsondePort             int      `json:"dropsonde_port,omitempty"`
	EnableLegacyAPIServer     bool     `json:"enable_legacy_api_endpoints"`
	EvacuationPollingInterval Duration `json:"evacuation_polling_interval,omitempty"`
	EvacuationTimeout         Duration `json:"evacuation_timeout,omitempty"`
	ListenAddr                string   `json:"listen_addr,omitempty"`
	ListenAddrAdmin           string   `json:"listen_addr_admin"`
	ListenAddrSecurable       string   `json:"listen_addr_securable,omitempty"`
	LockRetryInterval         Duration `json:"lock_retry_interval,omitempty"`
	LockTTL                   Duration `json:"lock_ttl,omitempty"`
	OptionalPlacementTags     []string `json:"optional_placement_tags"`
	PlacementTags             []string `json:"placement_tags"`
	PollingInterval           Duration `json:"polling_interval,omitempty"`
	PreloadedRootFS           StackMap `json:"preloaded_root_fs"`
	RequireTLS                bool     `json:"require_tls"`
	ServerCertFile            string   `json:"server_cert_file"`
	ServerKeyFile             string   `json:"server_key_file"`
	SessionName               string   `json:"session_name,omitempty"`
	SupportedProviders        []string `json:"supported_providers"`
	Zone                      string   `json:"zone"`
	debugserver.DebugServerConfig
	lagerflags.LagerConfig
	ExecutorConfig
}

type ExecutorConfig struct {
	CachePath                          string   `json:"cache_path,omitempty"`
	ContainerInodeLimit                uint64   `json:"container_inode_limit,omitempty"`
	ContainerMaxCpuShares              uint64   `json:"container_max_cpu_shares,omitempty"`
	ContainerMetricsReportInterval     Duration `json:"container_metrics_report_interval,omitempty"`
	ContainerOwnerName                 string   `json:"container_owner_name,omitempty"`
	ContainerReapInterval              Duration `json:"container_reap_interval,omitempty"`
	CreateWorkPoolSize                 int      `json:"create_work_pool_size,omitempty"`
	DeleteWorkPoolSize                 int      `json:"delete_work_pool_size,omitempty"`
	DiskMB                             string   `json:"disk_mb,omitempty"`
	ExportNetworkEnvVars               bool     `json:"export_network_env_vars,omitempty"`
	GardenAddr                         string   `json:"garden_addr,omitempty"`
	GardenHealthcheckCommandRetryPause Duration `json:"garden_healthcheck_command_retry_pause,omitempty"`
	GardenHealthcheckEmissionInterval  Duration `json:"garden_healthcheck_emission_interval,omitempty"`
	GardenHealthcheckInterval          Duration `json:"garden_healthcheck_interval,omitempty"`
	GardenHealthcheckProcessArgs       []string `json:"garden_healthcheck_process_args,omitempty"`
	GardenHealthcheckProcessDir        string   `json:"garden_healthcheck_process_dir"`
	GardenHealthcheckProcessEnv        []string `json:"garden_healthcheck_process_env,omitempty"`
	GardenHealthcheckProcessPath       string   `json:"garden_healthcheck_process_path"`
	GardenHealthcheckProcessUser       string   `json:"garden_healthcheck_process_user"`
	GardenHealthcheckTimeout           Duration `json:"garden_healthcheck_timeout,omitempty"`
	GardenNetwork                      string   `json:"garden_network,omitempty"`
	HealthCheckContainerOwnerName      string   `json:"healthcheck_container_owner_name,omitempty"`
	HealthCheckWorkPoolSize            int      `json:"healthcheck_work_pool_size,omitempty"`
	HealthyMonitoringInterval          Duration `json:"healthy_monitoring_interval,omitempty"`
	MaxCacheSizeInBytes                uint64   `json:"max_cache_size_in_bytes,omitempty"`
	MaxConcurrentDownloads             int      `json:"max_concurrent_downloads,omitempty"`
	MemoryMB                           string   `json:"memory_mb,omitempty"`
	MetricsWorkPoolSize                int      `json:"metrics_work_pool_size,omitempty"`
	PathToCACertsForDownloads          string   `json:"path_to_ca_certs_for_downloads"`
	PostSetupHook                      string   `json:"post_setup_hook"`
	PostSetupUser                      string   `json:"post_setup_user"`
	ReadWorkPoolSize                   int      `json:"read_work_pool_size,omitempty"`
	ReservedExpirationTime             Duration `json:"reserved_expiration_time,omitempty"`
	SkipCertVerify                     bool     `json:"skip_cert_verify,omitempty"`
	TempDir                            string   `json:"temp_dir,omitempty"`
	TrustedSystemCertificatesPath      string   `json:"trusted_system_certificates_path"`
	UnhealthyMonitoringInterval        Duration `json:"unhealthy_monitoring_interval,omitempty"`
	VolmanDriverPaths                  string   `json:"volman_driver_paths"`
}

func defaultConfig() RepConfig {
	return RepConfig{
		AdvertiseDomain:           "cell.service.cf.internal",
		BBSClientSessionCacheSize: 0,
		BBSMaxIdleConnsPerHost:    0,
		CommunicationTimeout:      Duration(10 * time.Second),
		DropsondePort:             3457,
		EnableLegacyAPIServer:     true,
		EvacuationPollingInterval: Duration(10 * time.Second),
		EvacuationTimeout:         Duration(10 * time.Minute),
		LagerConfig:               lagerflags.DefaultLagerConfig(),
		ListenAddr:                "0.0.0.0:1800",
		ListenAddrSecurable:       "0.0.0.0:1801",
		LockRetryInterval:         Duration(locket.RetryInterval),
		LockTTL:                   Duration(locket.DefaultSessionTTL),
		PollingInterval:           Duration(30 * time.Second),
		RequireTLS:                true,
		SessionName:               "rep",
		ExecutorConfig: ExecutorConfig{
			CachePath:                          "/tmp/cache",
			ContainerInodeLimit:                200000,
			ContainerMaxCpuShares:              0,
			ContainerMetricsReportInterval:     Duration(15 * time.Second),
			ContainerOwnerName:                 "executor",
			ContainerReapInterval:              Duration(time.Minute),
			CreateWorkPoolSize:                 32,
			DeleteWorkPoolSize:                 32,
			DiskMB:                             configuration.Automatic,
			ExportNetworkEnvVars:               false,
			GardenAddr:                         "/tmp/garden.sock",
			GardenHealthcheckCommandRetryPause: Duration(1 * time.Second),
			GardenHealthcheckEmissionInterval:  Duration(30 * time.Second),
			GardenHealthcheckInterval:          Duration(10 * time.Minute),
			GardenHealthcheckProcessArgs:       []string{},
			GardenHealthcheckProcessEnv:        []string{},
			GardenHealthcheckTimeout:           Duration(10 * time.Minute),
			GardenNetwork:                      "unix",
			HealthCheckContainerOwnerName:      "executor-health-check",
			HealthCheckWorkPoolSize:            64,
			HealthyMonitoringInterval:          Duration(30 * time.Second),
			MaxCacheSizeInBytes:                10 * 1024 * 1024 * 1024,
			MaxConcurrentDownloads:             5,
			MemoryMB:                           configuration.Automatic,
			MetricsWorkPoolSize:                8,
			ReadWorkPoolSize:                   64,
			ReservedExpirationTime:             Duration(time.Minute),
			SkipCertVerify:                     false,
			TempDir:                            "/tmp",
			UnhealthyMonitoringInterval:        Duration(500 * time.Millisecond),
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
