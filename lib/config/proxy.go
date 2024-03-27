// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"
	"os"
	"path/filepath"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tiproxy/lib/util/errors"
)

var (
	ErrUnsupportedProxyProtocolVersion = errors.New("unsupported proxy protocol version")
	ErrInvalidConfigValue              = errors.New("invalid config value")
)

type Config struct {
	Proxy    ProxyServer `yaml:"proxy,omitempty" toml:"proxy,omitempty" json:"proxy,omitempty"`
	API      API         `yaml:"api,omitempty" toml:"api,omitempty" json:"api,omitempty"`
	Advance  Advance     `yaml:"advance,omitempty" toml:"advance,omitempty" json:"advance,omitempty"`
	Workdir  string      `yaml:"workdir,omitempty" toml:"workdir,omitempty" json:"workdir,omitempty"`
	Security Security    `yaml:"security,omitempty" toml:"security,omitempty" json:"security,omitempty"`
	Log      Log         `yaml:"log,omitempty" toml:"log,omitempty" json:"log,omitempty"`
}

type KeepAlive struct {
	Enabled bool `yaml:"enabled,omitempty" toml:"enabled,omitempty" json:"enabled,omitempty"`
	// Idle, Cnt, and Intvl works only when the connection is idle. User packets will interrupt keep-alive.
	// If the peer crashes and doesn't send any packets, the connection will be closed within Idle+Cnt*Intvl.
	Idle  time.Duration `yaml:"idle,omitempty" toml:"idle,omitempty" json:"idle,omitempty"`
	Cnt   int           `yaml:"cnt,omitempty" toml:"cnt,omitempty" json:"cnt,omitempty"`
	Intvl time.Duration `yaml:"intvl,omitempty" toml:"intvl,omitempty" json:"intvl,omitempty"`
	// Timeout is the timeout of waiting ACK. It works for both user packets and keep-alive.
	// It is suggested to be equal or close to Cnt*Intvl.
	Timeout time.Duration `yaml:"timeout,omitempty" toml:"timeout,omitempty" json:"timeout,omitempty"`
}

type ProxyServerOnline struct {
	MaxConnections    uint64    `yaml:"max-connections,omitempty" toml:"max-connections,omitempty" json:"max-connections,omitempty"`
	ConnBufferSize    int       `yaml:"conn-buffer-size,omitempty" toml:"conn-buffer-size,omitempty" json:"conn-buffer-size,omitempty"`
	FrontendKeepalive KeepAlive `yaml:"frontend-keepalive" toml:"frontend-keepalive" json:"frontend-keepalive"`
	// BackendHealthyKeepalive applies when the observer treats the backend as healthy.
	// The config values should be conservative to save CPU and tolerate network fluctuation.
	BackendHealthyKeepalive KeepAlive `yaml:"backend-healthy-keepalive" toml:"backend-healthy-keepalive" json:"backend-healthy-keepalive"`
	// BackendUnhealthyKeepalive applies when the observer treats the backend as unhealthy.
	// The config values can be aggressive because the backend may stop anytime.
	BackendUnhealthyKeepalive  KeepAlive `yaml:"backend-unhealthy-keepalive" toml:"backend-unhealthy-keepalive" json:"backend-unhealthy-keepalive"`
	ProxyProtocol              string    `yaml:"proxy-protocol,omitempty" toml:"proxy-protocol,omitempty" json:"proxy-protocol,omitempty"`
	GracefulWaitBeforeShutdown int       `yaml:"graceful-wait-before-shutdown,omitempty" toml:"graceful-wait-before-shutdown,omitempty" json:"graceful-wait-before-shutdown,omitempty"`
	GracefulCloseConnTimeout   int       `yaml:"graceful-close-conn-timeout,omitempty" toml:"graceful-close-conn-timeout,omitempty" json:"graceful-close-conn-timeout,omitempty"`
}

type ProxyServer struct {
	Addr              string `yaml:"addr,omitempty" toml:"addr,omitempty" json:"addr,omitempty"`
	AdvertiseAddr     string `yaml:"advertise-addr,omitempty" toml:"advertise-addr,omitempty" json:"advertise-addr,omitempty"`
	PDAddrs           string `yaml:"pd-addrs,omitempty" toml:"pd-addrs,omitempty" json:"pd-addrs,omitempty"`
	ProxyServerOnline `yaml:",inline" toml:",inline" json:",inline"`
}

type API struct {
	Addr          string `yaml:"addr,omitempty" toml:"addr,omitempty" json:"addr,omitempty"`
	ProxyProtocol string `yaml:"proxy-protocol,omitempty" toml:"proxy-protocol,omitempty" json:"proxy-protocol,omitempty"`
}

type Advance struct {
	IgnoreWrongNamespace bool `yaml:"ignore-wrong-namespace,omitempty" toml:"ignore-wrong-namespace,omitempty" json:"ignore-wrong-namespace,omitempty"`
}

type LogOnline struct {
	Level   string  `yaml:"level,omitempty" toml:"level,omitempty" json:"level,omitempty"`
	LogFile LogFile `yaml:"log-file,omitempty" toml:"log-file,omitempty" json:"log-file,omitempty"`
}

type Log struct {
	Encoder   string `yaml:"encoder,omitempty" toml:"encoder,omitempty" json:"encoder,omitempty"`
	LogOnline `yaml:",inline" toml:",inline" json:",inline"`
}

type LogFile struct {
	Filename   string `yaml:"filename,omitempty" toml:"filename,omitempty" json:"filename,omitempty"`
	MaxSize    int    `yaml:"max-size,omitempty" toml:"max-size,omitempty" json:"max-size,omitempty"`
	MaxDays    int    `yaml:"max-days,omitempty" toml:"max-days,omitempty" json:"max-days,omitempty"`
	MaxBackups int    `yaml:"max-backups,omitempty" toml:"max-backups,omitempty" json:"max-backups,omitempty"`
}

type TLSConfig struct {
	Cert               string `yaml:"cert,omitempty" toml:"cert,omitempty" json:"cert,omitempty"`
	Key                string `yaml:"key,omitempty" toml:"key,omitempty" json:"key,omitempty"`
	CA                 string `yaml:"ca,omitempty" toml:"ca,omitempty" json:"ca,omitempty"`
	MinTLSVersion      string `yaml:"min-tls-version,omitempty" toml:"min-tls-version,omitempty" json:"min-tls-version,omitempty"`
	AutoCerts          bool   `yaml:"auto-certs,omitempty" toml:"auto-certs,omitempty" json:"auto-certs,omitempty"`
	RSAKeySize         int    `yaml:"rsa-key-size,omitempty" toml:"rsa-key-size,omitempty" json:"rsa-key-size,omitempty"`
	AutoExpireDuration string `yaml:"autocert-expire-duration,omitempty" toml:"autocert-expire-duration,omitempty" json:"autocert-expire-duration,omitempty"`
	SkipCA             bool   `yaml:"skip-ca,omitempty" toml:"skip-ca,omitempty" json:"skip-ca,omitempty"`
}

func (c TLSConfig) HasCert() bool {
	return !(c.Cert == "" && c.Key == "")
}

func (c TLSConfig) HasCA() bool {
	return c.CA != ""
}

type Security struct {
	ServerSQLTLS      TLSConfig `yaml:"server-tls,omitempty" toml:"server-tls,omitempty" json:"server-tls,omitempty"`
	ServerHTTPTLS     TLSConfig `yaml:"server-http-tls,omitempty" toml:"server-http-tls,omitempty" json:"server-http-tls,omitempty"`
	ClusterTLS        TLSConfig `yaml:"cluster-tls,omitempty" toml:"cluster-tls,omitempty" json:"cluster-tls,omitempty"`
	SQLTLS            TLSConfig `yaml:"sql-tls,omitempty" toml:"sql-tls,omitempty" json:"sql-tls,omitempty"`
	RequireBackendTLS bool      `yaml:"require-backend-tls,omitempty" toml:"require-backend-tls,omitempty" json:"require-backend-tls,omitempty"`
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

	cfg.Advance.IgnoreWrongNamespace = true
	cfg.Security.SQLTLS.MinTLSVersion = "1.2"
	cfg.Security.ServerSQLTLS.MinTLSVersion = "1.2"
	cfg.Security.ServerHTTPTLS.MinTLSVersion = "1.2"
	cfg.Security.ClusterTLS.MinTLSVersion = "1.2"

	return &cfg
}

func (cfg *Config) Clone() *Config {
	newCfg := *cfg
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

	return nil
}

func (cfg *Config) ToBytes() ([]byte, error) {
	b := new(bytes.Buffer)
	err := toml.NewEncoder(b).Encode(cfg)
	return b.Bytes(), errors.WithStack(err)
}
