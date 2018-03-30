package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"code.cloudfoundry.org/debugserver"
	loggingclient "code.cloudfoundry.org/diego-logging-client"
	"code.cloudfoundry.org/durationjson"
	executorinit "code.cloudfoundry.org/executor/initializer"
	"code.cloudfoundry.org/executor/model"
	"code.cloudfoundry.org/lager/lagerflags"
	"code.cloudfoundry.org/locket"
	"code.cloudfoundry.org/rep"
	vcmodels "github.com/virtualcloudfoundry/vcontainercommon/vcontainermodels"
)

type RootFS struct {
	Name, Path string
}

type RootFSes []RootFS

func (m *RootFSes) UnmarshalJSON(data []byte) error {
	*m = make(RootFSes, 0)
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

		*m = append(*m, RootFS{parts[0], parts[1]})
	}

	return nil
}

func (rootFSes RootFSes) Names() []string {
	names := make([]string, len(rootFSes))
	for i, rootFS := range rootFSes {
		names[i] = rootFS.Name
	}
	return names
}

func (rootFSes RootFSes) StackPathMap() rep.StackPathMap {
	m := make(rep.StackPathMap)
	for _, rootFS := range rootFSes {
		m[rootFS.Name] = rootFS.Path
	}
	return m
}

func (m RootFSes) MarshalJSON() (b []byte, err error) {
	arr := make([]string, len(m))
	for i, rootFS := range m {
		arr[i] = fmt.Sprintf("%s:%s", rootFS.Name, rootFS.Path)
	}
	return json.Marshal(arr)
}

type RepConfig struct {
	AdvertiseDomain                 string                `json:"advertise_domain,omitempty"`
	BBSAddress                      string                `json:"bbs_address"`
	BBSClientSessionCacheSize       int                   `json:"bbs_client_session_cache_size,omitempty"`
	BBSMaxIdleConnsPerHost          int                   `json:"bbs_max_idle_conns_per_host,omitempty"`
	BBSCACertFile                   string                `json:"bbs_ca_cert_file"`     // DEPRECATED. Kept around for dusts compatability
	BBSClientCertFile               string                `json:"bbs_client_cert_file"` // DEPRECATED. Kept around for dusts compatability
	BBSClientKeyFile                string                `json:"bbs_client_key_file"`  // DEPRECATED. Kept around for dusts compatability
	CaCertFile                      string                `json:"ca_cert_file"`
	CellID                          string                `json:"cell_id"`
	CommunicationTimeout            durationjson.Duration `json:"communication_timeout,omitempty"`
	ConsulCACert                    string                `json:"consul_ca_cert"`
	ConsulClientCert                string                `json:"consul_client_cert"`
	ConsulClientKey                 string                `json:"consul_client_key"`
	ConsulCluster                   string                `json:"consul_cluster"`
	EnableConsulServiceRegistration bool                  `json:"enable_consul_service_registration,omitempty"`
	EvacuationPollingInterval       durationjson.Duration `json:"evacuation_polling_interval,omitempty"`
	EvacuationTimeout               durationjson.Duration `json:"evacuation_timeout,omitempty"`
	ListenAddr                      string                `json:"listen_addr,omitempty"`
	ListenAddrSecurable             string                `json:"listen_addr_securable,omitempty"`
	LockRetryInterval               durationjson.Duration `json:"lock_retry_interval,omitempty"`
	LockTTL                         durationjson.Duration `json:"lock_ttl,omitempty"`
	OptionalPlacementTags           []string              `json:"optional_placement_tags"`
	PlacementTags                   []string              `json:"placement_tags"`
	PollingInterval                 durationjson.Duration `json:"polling_interval,omitempty"`
	PreloadedRootFS                 RootFSes              `json:"preloaded_root_fs"`
	ServerCertFile                  string                `json:"server_cert_file"` // DEPRECATED. Kept around for dusts compatability
	ServerKeyFile                   string                `json:"server_key_file"`  // DEPRECATED. Kept around for dusts compatability
	CertFile                        string                `json:"cert_file"`
	KeyFile                         string                `json:"key_file"`
	SessionName                     string                `json:"session_name,omitempty"`
	SupportedProviders              []string              `json:"supported_providers"`
	Zone                            string                `json:"zone"`
	LoggregatorConfig               loggingclient.Config  `json:"loggregator"`
	CellRegistrationsLocketEnabled  bool                  `json:"cell_registrations_locket_enabled"`
	debugserver.DebugServerConfig
	model.ExecutorConfig
	lagerflags.LagerConfig
	locket.ClientLocketConfig
	vcmodels.VContainerClientConfig
}

func defaultConfig() RepConfig {
	return RepConfig{
		AdvertiseDomain:           "cell.service.cf.internal",
		BBSClientSessionCacheSize: 0,
		BBSMaxIdleConnsPerHost:    0,
		CommunicationTimeout:      durationjson.Duration(10 * time.Second),
		EvacuationPollingInterval: durationjson.Duration(10 * time.Second),
		EvacuationTimeout:         durationjson.Duration(10 * time.Minute),
		ExecutorConfig:            executorinit.DefaultConfiguration,
		LagerConfig:               lagerflags.DefaultLagerConfig(),
		ListenAddr:                "0.0.0.0:1800",
		ListenAddrSecurable:       "0.0.0.0:1801",
		LockRetryInterval:         durationjson.Duration(locket.RetryInterval),
		LockTTL:                   durationjson.Duration(locket.DefaultSessionTTL),
		PollingInterval:           durationjson.Duration(30 * time.Second),
		SessionName:               "rep",
	}
}

func NewRepConfig(configPath string) (RepConfig, error) {
	repConfig := defaultConfig()
	configFile, err := os.Open(configPath)
	if err != nil {
		return RepConfig{}, err
	}

	defer configFile.Close()

	decoder := json.NewDecoder(configFile)

	err = decoder.Decode(&repConfig)
	if err != nil {
		return RepConfig{}, err
	}

	return repConfig, nil
}
