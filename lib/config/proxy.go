// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
)

var (
	ErrUnsupportedProxyProtocolVersion = errors.New("unsupported proxy protocol version")
	ErrInvalidConfigValue              = errors.New("invalid config value")
)

type Config struct {
	Proxy               ProxyServer           `yaml:"proxy,omitempty" toml:"proxy,omitempty" json:"proxy,omitempty"`
	API                 API                   `yaml:"api,omitempty" toml:"api,omitempty" json:"api,omitempty"`
	Workdir             string                `yaml:"workdir,omitempty" toml:"workdir,omitempty" json:"workdir,omitempty" reloadable:"false"`
	Security            Security              `yaml:"security,omitempty" toml:"security,omitempty" json:"security,omitempty"`
	Log                 Log                   `yaml:"log,omitempty" toml:"log,omitempty" json:"log,omitempty"`
	Balance             Balance               `yaml:"balance,omitempty" toml:"balance,omitempty" json:"balance,omitempty"`
	Labels              map[string]string     `yaml:"labels,omitempty" toml:"labels,omitempty" json:"labels,omitempty" reloadable:"true"`
	HA                  HA                    `yaml:"ha,omitempty" toml:"ha,omitempty" json:"ha,omitempty"`
	Metering            config.MeteringConfig `yaml:"metering,omitempty" toml:"metering,omitempty" json:"metering,omitempty" reloadable:"false"`
	EnableTrafficReplay bool                  `yaml:"enable-traffic-replay,omitempty" toml:"enable-traffic-replay,omitempty" json:"enable-traffic-replay,omitempty" reloadable:"true"`
}

type KeepAlive struct {
	Enabled bool `yaml:"enabled,omitempty" toml:"enabled,omitempty" json:"enabled,omitempty" reloadable:"true"`
	// Idle, Cnt, and Intvl works only when the connection is idle. User packets will interrupt keep-alive.
	// If the peer crashes and doesn't send any packets, the connection will be closed within Idle+Cnt*Intvl.
	Idle  time.Duration `yaml:"idle,omitempty" toml:"idle,omitempty" json:"idle,omitempty" reloadable:"true"`
	Cnt   int           `yaml:"cnt,omitempty" toml:"cnt,omitempty" json:"cnt,omitempty" reloadable:"true"`
	Intvl time.Duration `yaml:"intvl,omitempty" toml:"intvl,omitempty" json:"intvl,omitempty" reloadable:"true"`
	// Timeout is the timeout of waiting ACK. It works for both user packets and keep-alive.
	// It is suggested to be equal or close to Cnt*Intvl.
	Timeout time.Duration `yaml:"timeout,omitempty" toml:"timeout,omitempty" json:"timeout,omitempty" reloadable:"true"`
}

type ProxyServerOnline struct {
	MaxConnections    uint64    `yaml:"max-connections,omitempty" toml:"max-connections,omitempty" json:"max-connections,omitempty" reloadable:"true"`
	ConnBufferSize    int       `yaml:"conn-buffer-size,omitempty" toml:"conn-buffer-size,omitempty" json:"conn-buffer-size,omitempty" reloadable:"true"`
	FrontendKeepalive KeepAlive `yaml:"frontend-keepalive" toml:"frontend-keepalive" json:"frontend-keepalive"`
	// BackendHealthyKeepalive applies when the observer treats the backend as healthy.
	// The config values should be conservative to save CPU and tolerate network fluctuation.
	BackendHealthyKeepalive KeepAlive `yaml:"backend-healthy-keepalive" toml:"backend-healthy-keepalive" json:"backend-healthy-keepalive"`
	// BackendUnhealthyKeepalive applies when the observer treats the backend as unhealthy.
	// The config values can be aggressive because the backend may stop anytime.
	BackendUnhealthyKeepalive KeepAlive `yaml:"backend-unhealthy-keepalive" toml:"backend-unhealthy-keepalive" json:"backend-unhealthy-keepalive"`
	ProxyProtocol             string    `yaml:"proxy-protocol,omitempty" toml:"proxy-protocol,omitempty" json:"proxy-protocol,omitempty" reloadable:"true"`
	// In k8s, the pod terminationGracePeriodSeconds can be set to very long so that these configs can be updated online.
	GracefulWaitBeforeShutdown int `yaml:"graceful-wait-before-shutdown,omitempty" toml:"graceful-wait-before-shutdown,omitempty" json:"graceful-wait-before-shutdown,omitempty" reloadable:"true"`
	GracefulCloseConnTimeout   int `yaml:"graceful-close-conn-timeout,omitempty" toml:"graceful-close-conn-timeout,omitempty" json:"graceful-close-conn-timeout,omitempty" reloadable:"true"`
	// Public and private traffic are metered separately.
	PublicEndpoints []string `yaml:"public-endpoints,omitempty" toml:"public-endpoints,omitempty" json:"public-endpoints,omitempty" reloadable:"true"`
	// BackendClusters represents multiple backend clusters that the proxy can route to. It can be reloaded
	// online.
	BackendClusters []BackendCluster `yaml:"backend-clusters,omitempty" toml:"backend-clusters,omitempty" json:"backend-clusters,omitempty" reloadable:"true"`
}

type ProxyServer struct {
	Addr              string `yaml:"addr,omitempty" toml:"addr,omitempty" json:"addr,omitempty" reloadable:"false"`
	AdvertiseAddr     string `yaml:"advertise-addr,omitempty" toml:"advertise-addr,omitempty" json:"advertise-addr,omitempty" reloadable:"false"`
	PDAddrs           string `yaml:"pd-addrs,omitempty" toml:"pd-addrs,omitempty" json:"pd-addrs,omitempty" reloadable:"false"`
	PortRange         []int  `yaml:"port-range,omitempty" toml:"port-range,omitempty" json:"port-range,omitempty" reloadable:"false"`
	ProxyServerOnline `yaml:",inline" toml:",inline" json:",inline"`
}

type BackendCluster struct {
	Name      string   `yaml:"name,omitempty" toml:"name,omitempty" json:"name,omitempty" reloadable:"true"`
	PDAddrs   string   `yaml:"pd-addrs,omitempty" toml:"pd-addrs,omitempty" json:"pd-addrs,omitempty" reloadable:"true"`
	NSServers []string `yaml:"ns-servers,omitempty" toml:"ns-servers,omitempty" json:"ns-servers,omitempty" reloadable:"true"`
}

type API struct {
	Addr          string `yaml:"addr,omitempty" toml:"addr,omitempty" json:"addr,omitempty" reloadable:"false"`
	ProxyProtocol string `yaml:"proxy-protocol,omitempty" toml:"proxy-protocol,omitempty" json:"proxy-protocol,omitempty" reloadable:"false"`
}

type LogOnline struct {
	Level   string  `yaml:"level,omitempty" toml:"level,omitempty" json:"level,omitempty" reloadable:"true"`
	LogFile LogFile `yaml:"log-file,omitempty" toml:"log-file,omitempty" json:"log-file,omitempty"`
}

type Log struct {
	Encoder   string `yaml:"encoder,omitempty" toml:"encoder,omitempty" json:"encoder,omitempty" reloadable:"false"`
	Simple    bool   `yaml:"simple,omitempty" toml:"simple,omitempty" json:"simple,omitempty" reloadable:"false"`
	LogOnline `yaml:",inline" toml:",inline" json:",inline"`
}

type LogFile struct {
	Filename   string `yaml:"filename,omitempty" toml:"filename,omitempty" json:"filename,omitempty" reloadable:"true"`
	MaxSize    int    `yaml:"max-size,omitempty" toml:"max-size,omitempty" json:"max-size,omitempty" reloadable:"true"`
	MaxDays    int    `yaml:"max-days,omitempty" toml:"max-days,omitempty" json:"max-days,omitempty" reloadable:"true"`
	MaxBackups int    `yaml:"max-backups,omitempty" toml:"max-backups,omitempty" json:"max-backups,omitempty" reloadable:"true"`
}

type HA struct {
	VirtualIP           string        `yaml:"virtual-ip,omitempty" toml:"virtual-ip,omitempty" json:"virtual-ip,omitempty" reloadable:"false"`
	Interface           string        `yaml:"interface,omitempty" toml:"interface,omitempty" json:"interface,omitempty" reloadable:"false"`
	GARPBurstCount      int           `yaml:"garp-burst-count,omitempty" toml:"garp-burst-count,omitempty" json:"garp-burst-count,omitempty" reloadable:"false"`
	GARPBurstInterval   time.Duration `yaml:"garp-burst-interval,omitempty" toml:"garp-burst-interval,omitempty" json:"garp-burst-interval,omitempty" reloadable:"false"`
	GARPRefreshInterval time.Duration `yaml:"garp-refresh-interval,omitempty" toml:"garp-refresh-interval,omitempty" json:"garp-refresh-interval,omitempty" reloadable:"false"`
}

func DefaultKeepAlive() (frontend, backendHealthy, backendUnhealthy KeepAlive) {
	frontend.Enabled = true
	backendHealthy.Enabled = true
	backendHealthy.Idle = 60 * time.Second
	backendHealthy.Cnt = 5
	backendHealthy.Intvl = 3 * time.Second
	backendHealthy.Timeout = 15 * time.Second
	backendUnhealthy.Enabled = true
	backendUnhealthy.Idle = 10 * time.Second
	backendUnhealthy.Cnt = 5
	backendUnhealthy.Intvl = 1 * time.Second
	backendUnhealthy.Timeout = 5 * time.Second
	return
}

func NewConfig() *Config {
	var cfg Config

	cfg.Proxy.Addr = "0.0.0.0:6000"
	cfg.Proxy.FrontendKeepalive, cfg.Proxy.BackendHealthyKeepalive, cfg.Proxy.BackendUnhealthyKeepalive = DefaultKeepAlive()
	cfg.Proxy.PDAddrs = "127.0.0.1:2379"
	cfg.Proxy.GracefulCloseConnTimeout = 15

	cfg.API.Addr = "0.0.0.0:3080"

	cfg.Log.Level = "info"
	cfg.Log.Encoder = "tidb"
	cfg.Log.LogFile.MaxSize = 300
	cfg.Log.LogFile.MaxDays = 3
	cfg.Log.LogFile.MaxBackups = 3

	cfg.Security.SQLTLS.MinTLSVersion = "1.2"
	cfg.Security.ServerSQLTLS.MinTLSVersion = "1.2"
	cfg.Security.ServerHTTPTLS.MinTLSVersion = "1.2"
	cfg.Security.ClusterTLS.MinTLSVersion = "1.2"

	cfg.Balance = DefaultBalance()

	cfg.HA.GARPBurstCount = 5

	cfg.EnableTrafficReplay = true

	return &cfg
}

func (cfg *Config) Clone() *Config {
	newCfg := *cfg
	newCfg.Labels = maps.Clone(cfg.Labels)
	newCfg.Proxy.PublicEndpoints = slices.Clone(cfg.Proxy.PublicEndpoints)
	newCfg.Proxy.BackendClusters = slices.Clone(cfg.Proxy.BackendClusters)
	for i := range newCfg.Proxy.BackendClusters {
		newCfg.Proxy.BackendClusters[i].NSServers = slices.Clone(newCfg.Proxy.BackendClusters[i].NSServers)
	}
	return &newCfg
}

func (cfg *Config) Check() error {
	if cfg.Workdir == "" {
		d, err := os.Getwd()
		if err != nil {
			return errors.WithStack(err)
		}
		cfg.Workdir = filepath.Clean(filepath.Join(d, "work"))
	}

	switch cfg.Proxy.ProxyProtocol {
	case "v2":
	case "":
	default:
		return errors.Wrapf(ErrUnsupportedProxyProtocolVersion, "%s", cfg.Proxy.ProxyProtocol)
	}

	if cfg.Proxy.ConnBufferSize > 0 && (cfg.Proxy.ConnBufferSize > 16*1024*1024 || cfg.Proxy.ConnBufferSize < 1024) {
		return errors.Wrapf(ErrInvalidConfigValue, "conn-buffer-size must be between 1K and 16M")
	}
	if err := cfg.Proxy.Check(); err != nil {
		return err
	}

	if err := cfg.Balance.Check(); err != nil {
		return err
	}
	if cfg.HA.GARPBurstCount < 0 {
		return errors.Wrapf(ErrInvalidConfigValue, "ha.garp-burst-count must be greater than or equal to 0")
	}
	if cfg.HA.GARPBurstInterval < 0 {
		return errors.Wrapf(ErrInvalidConfigValue, "ha.garp-burst-interval must be greater than or equal to 0")
	}
	if cfg.HA.GARPRefreshInterval < 0 {
		return errors.Wrapf(ErrInvalidConfigValue, "ha.garp-refresh-interval must be greater than or equal to 0")
	}
	if cfg.HA.GARPBurstCount == 0 && cfg.HA.GARPRefreshInterval > 0 {
		return errors.Wrapf(ErrInvalidConfigValue, "ha.garp-burst-count must be greater than 0 when ha.garp-refresh-interval is enabled")
	}

	return nil
}

func (cfg *Config) ToBytes() ([]byte, error) {
	b := new(bytes.Buffer)
	err := toml.NewEncoder(b).Encode(cfg)
	return b.Bytes(), errors.WithStack(err)
}

func (cfg *Config) GetIPPort() (ip, port, statusPort string, err error) {
	addrs, err := cfg.Proxy.GetSQLAddrs()
	if err != nil {
		return
	}
	ip, port, err = net.SplitHostPort(addrs[0])
	if err != nil {
		return
	}
	_, statusPort, err = net.SplitHostPort(cfg.API.Addr)
	if err != nil {
		return
	}
	// AdvertiseAddr may be a DNS in k8s and certificate SAN typically contains DNS but not IP.
	if len(cfg.Proxy.AdvertiseAddr) > 0 {
		ip = cfg.Proxy.AdvertiseAddr
	} else {
		// reporting a non unicast IP makes no sense, try to find one
		// loopback/linklocal-unicast are not global unicast IP, but are valid local unicast IP
		if pip := net.ParseIP(ip); ip == "" || pip.Equal(net.IPv4bcast) || pip.IsUnspecified() || pip.IsMulticast() {
			if addrs, err := net.InterfaceAddrs(); err == nil {
				for _, address := range addrs {
					if ipnet, ok := address.(*net.IPNet); ok && ipnet.IP.IsGlobalUnicast() {
						ipStr := ipnet.IP.String()
						// filter virtual IP
						if !strings.HasPrefix(cfg.HA.VirtualIP, ipStr) {
							ip = ipStr
							break
						}
					}
				}
			}
		}
	}
	return
}

// GetBackendClusters returns configured backend clusters.
// It keeps backward compatibility for the legacy `proxy.pd-addrs` setting.
func (cfg *Config) GetBackendClusters() []BackendCluster {
	if len(cfg.Proxy.BackendClusters) > 0 {
		return cfg.Proxy.BackendClusters
	}
	if strings.TrimSpace(cfg.Proxy.PDAddrs) == "" {
		return nil
	}
	return []BackendCluster{{
		Name:    "default",
		PDAddrs: cfg.Proxy.PDAddrs,
	}}
}

func (ps *ProxyServer) Check() error {
	if _, err := ps.GetSQLAddrs(); err != nil {
		return errors.Wrapf(ErrInvalidConfigValue, "invalid proxy.addr or proxy.port-range: %s", err.Error())
	}
	if len(ps.BackendClusters) == 0 {
		return nil
	}

	clusterNames := make(map[string]struct{}, len(ps.BackendClusters))
	for i, cluster := range ps.BackendClusters {
		name := strings.TrimSpace(cluster.Name)
		if name == "" {
			return errors.Wrapf(ErrInvalidConfigValue, "proxy.backend-clusters[%d].name is empty", i)
		}
		if _, ok := clusterNames[name]; ok {
			return errors.Wrapf(ErrInvalidConfigValue, "duplicate proxy.backend-clusters name %s", name)
		}
		clusterNames[name] = struct{}{}
		if err := validateAddrList(cluster.PDAddrs, "proxy.backend-clusters.pd-addrs"); err != nil {
			return err
		}
		if _, err := ParseNSServers(cluster.NSServers); err != nil {
			return errors.Wrapf(ErrInvalidConfigValue, "invalid proxy.backend-clusters.ns-servers: %s", err.Error())
		}
	}
	return nil
}

// SplitAddrList splits a comma-separated address list, trims each address, and drops empty entries.
func SplitAddrList(addrs string) []string {
	parts := strings.Split(addrs, ",")
	trimmed := make([]string, 0, len(parts))
	for _, part := range parts {
		addr := strings.TrimSpace(part)
		if addr != "" {
			trimmed = append(trimmed, addr)
		}
	}
	return trimmed
}

func validateAddrList(addrs, field string) error {
	parts := SplitAddrList(addrs)
	if len(parts) == 0 {
		return errors.Wrapf(ErrInvalidConfigValue, "%s is empty", field)
	}
	for _, addr := range parts {
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return errors.Wrapf(ErrInvalidConfigValue, "invalid %s address %s", field, addr)
		}
	}
	return nil
}

func ParseNSServers(nsServers []string) ([]string, error) {
	if len(nsServers) == 0 {
		return nil, nil
	}
	normalized := make([]string, 0, len(nsServers))
	for _, server := range nsServers {
		addr, err := normalizeNSServer(server)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, addr)
	}
	return normalized, nil
}

func normalizeNSServer(server string) (string, error) {
	host, port, err := net.SplitHostPort(server)
	if err == nil {
		if host == "" {
			return "", errors.Wrapf(ErrInvalidConfigValue, "host is empty")
		}
		portNum, err := strconv.Atoi(port)
		if err != nil || portNum < 1 || portNum > 65535 {
			return "", errors.Wrapf(ErrInvalidConfigValue, "port is invalid")
		}
		return net.JoinHostPort(host, strconv.Itoa(portNum)), nil
	}

	if server == "" {
		return "", errors.Wrapf(ErrInvalidConfigValue, "host is empty")
	}
	if strings.ContainsAny(server, "[]") {
		return "", errors.Wrapf(ErrInvalidConfigValue, "host is invalid")
	}
	return net.JoinHostPort(server, "53"), nil
}

func (ps *ProxyServer) GetSQLAddrs() ([]string, error) {
	addrs := SplitAddrList(ps.Addr)
	if len(addrs) == 0 {
		if len(ps.PortRange) == 0 {
			return []string{""}, nil
		}
		return nil, errors.Wrapf(ErrInvalidConfigValue, "proxy.addr is empty")
	}
	if len(ps.PortRange) == 0 {
		return addrs, nil
	}
	if len(ps.PortRange) != 2 {
		return nil, errors.Wrapf(ErrInvalidConfigValue, "proxy.port-range must contain exactly two ports")
	}
	start, end := ps.PortRange[0], ps.PortRange[1]
	if start < 1 || start > 65535 || end < 1 || end > 65535 || start > end {
		return nil, errors.Wrapf(ErrInvalidConfigValue, "proxy.port-range is invalid")
	}
	if len(addrs) != 1 {
		return nil, errors.Wrapf(ErrInvalidConfigValue, "proxy.addr must contain exactly one host when proxy.port-range is set")
	}
	host, _, err := net.SplitHostPort(addrs[0])
	if err != nil {
		return nil, errors.Wrapf(ErrInvalidConfigValue, "invalid proxy.addr: %s", err.Error())
	}
	sqlAddrs := make([]string, 0, end-start+1)
	for port := start; port <= end; port++ {
		sqlAddrs = append(sqlAddrs, net.JoinHostPort(host, strconv.Itoa(port)))
	}
	return sqlAddrs, nil
}
