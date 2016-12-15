package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"code.cloudfoundry.org/debugserver"
	executorinit "code.cloudfoundry.org/executor/initializer"
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
	executorinit.ExecutorConfig
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
		ExecutorConfig:            executorinit.DefaultConfiguration,
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
