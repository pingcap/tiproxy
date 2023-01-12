// Copyright 2020 Ipalfish, Inc.
// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"bytes"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/TiProxy/lib/util/errors"
)

var (
	ErrUnsupportedProxyProtocolVersion = errors.New("unsupported proxy protocol version")
)

type Config struct {
	Proxy    ProxyServer `yaml:"proxy,omitempty" toml:"proxy,omitempty" json:"proxy,omitempty"`
	API      API         `yaml:"api,omitempty" toml:"api,omitempty" json:"api,omitempty"`
	Advance  Advance     `yaml:"advance,omitempty" toml:"advance,omitempty" json:"advance,omitempty"`
	Workdir  string      `yaml:"workdir,omitempty" toml:"workdir,omitempty" json:"workdir,omitempty"`
	Security Security    `yaml:"security,omitempty" toml:"security,omitempty" json:"security,omitempty"`
	Metrics  Metrics     `yaml:"metrics,omitempty" toml:"metrics,omitempty" json:"metrics,omitempty"`
	Log      Log         `yaml:"log,omitempty" toml:"log,omitempty" json:"log,omitempty"`
}

type Metrics struct {
	MetricsAddr     string `toml:"metrics-addr" json:"metrics-addr"`
	MetricsInterval uint   `toml:"metrics-interval" json:"metrics-interval"`
}

type ProxyServerOnline struct {
	MaxConnections             uint64 `yaml:"max-connections,omitempty" toml:"max-connections,omitempty" json:"max-connections,omitempty"`
	TCPKeepAlive               bool   `yaml:"tcp-keep-alive,omitempty" toml:"tcp-keep-alive,omitempty" json:"tcp-keep-alive,omitempty"`
	ProxyProtocol              string `yaml:"proxy-protocol,omitempty" toml:"proxy-protocol,omitempty" json:"proxy-protocol,omitempty"`
	GracefulWaitBeforeShutdown int    `yaml:"graceful-wait-before-shutdown,omitempty" toml:"graceful-wait-before-shutdown,omitempty" json:"graceful-wait-before-shutdown,omitempty"`
}

type ProxyServer struct {
	Addr              string `yaml:"addr,omitempty" toml:"addr,omitempty" json:"addr,omitempty"`
	PDAddrs           string `yaml:"pd-addrs,omitempty" toml:"pd-addrs,omitempty" json:"pd-addrs,omitempty"`
	RequireBackendTLS bool   `yaml:"require-backend-tls,omitempty" toml:"require-backend-tls,omitempty" json:"require-backend-tls,omitempty"`
	ProxyServerOnline `yaml:",inline" toml:",inline" json:",inline"`
}

type API struct {
	Addr            string `yaml:"addr,omitempty" toml:"addr,omitempty" json:"addr,omitempty"`
	User            string `yaml:"user,omitempty" toml:"user,omitempty" json:"user,omitempty"`
	Password        string `yaml:"password,omitempty" toml:"password,omitempty" json:"password,omitempty"`
	EnableBasicAuth bool   `yaml:"enable-basic-auth,omitempty" toml:"enable-basic-auth,omitempty" json:"enable-basic-auth,omitempty"`
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
	ServerTLS  TLSConfig `yaml:"server-tls,omitempty" toml:"server-tls,omitempty" json:"server-tls,omitempty"`
	PeerTLS    TLSConfig `yaml:"peer-tls,omitempty" toml:"peer-tls,omitempty" json:"peer-tls,omitempty"`
	ClusterTLS TLSConfig `yaml:"cluster-tls,omitempty" toml:"cluster-tls,omitempty" json:"cluster-tls,omitempty"`
	SQLTLS     TLSConfig `yaml:"sql-tls,omitempty" toml:"sql-tls,omitempty" json:"sql-tls,omitempty"`
}

func NewConfig() *Config {
	var cfg Config

	cfg.Proxy.Addr = "0.0.0.0:6000"
	cfg.Proxy.TCPKeepAlive = true
	cfg.Proxy.RequireBackendTLS = true
	cfg.Proxy.PDAddrs = "127.0.0.1:2379"

	cfg.API.Addr = "0.0.0.0:3080"

	cfg.Log.Level = "info"
	cfg.Log.Encoder = "tidb"
	cfg.Log.LogFile.MaxSize = 300
	cfg.Log.LogFile.MaxDays = 3
	cfg.Log.LogFile.MaxBackups = 3

	cfg.Advance.IgnoreWrongNamespace = true

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
			return err
		}
		cfg.Workdir = filepath.Clean(filepath.Join(d, "work"))
	}

	switch cfg.Proxy.ProxyProtocol {
	case "v2":
	case "":
	default:
		return errors.Wrapf(ErrUnsupportedProxyProtocolVersion, "%s", cfg.Proxy.ProxyProtocol)
	}

	return nil
}

func (cfg *Config) ToBytes() ([]byte, error) {
	b := new(bytes.Buffer)
	err := toml.NewEncoder(b).Encode(cfg)
	return b.Bytes(), err
}
