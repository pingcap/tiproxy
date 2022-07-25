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
	"net/url"
	"os"
	"path/filepath"

	"github.com/goccy/go-yaml"
)

const (
	DefaultClusterName = "default"
)

type Config struct {
	Workdir string    `yaml:"workdir"`
	LCUrls  []url.URL `yaml:"listen-urls"`
	ACUrls  []url.URL `yaml:"advertise-urls"`
	LPUrls  []url.URL `yaml:"listen-peer-urls"`
	APUrls  []url.URL `yaml:"advertise-peer-urls"`

	Config   ConfigManager `yaml:"config"`
	Proxy    ProxyServer   `yaml:"proxy"`
	API      API           `yaml:"api"`
	Metrics  Metrics       `yaml:"metrics"`
	Log      Log           `yaml:"log"`
	Security Security      `yaml:"security"`
}

type Metrics struct {
	PromCluster string `yaml:"prom_cluster"`
}

type ConfigManager struct {
	IgnoreWrongNamespace bool `yaml:"ignore_wrong_namespace"`
}

type ProxyServer struct {
	Addr           string `yaml:"addr"`
	MaxConnections uint32 `yaml:"max_connections"`
	TCPKeepAlive   bool   `yaml:"tcp_keep_alive"`
	PDAddrs        string `yaml:"pd_addrs"`
}

type API struct {
	EnableBasicAuth bool   `yaml:"enable_basic_auth"`
	User            string `yaml:"user"`
	Password        string `yaml:"password"`
}

type Log struct {
	Level   string  `yaml:"level"`
	Encoder string  `yaml:"encoder"`
	LogFile LogFile `yaml:"log_file"`
}

type LogFile struct {
	Filename   string `yaml:"filename"`
	MaxSize    int    `yaml:"max_size"`
	MaxDays    int    `yaml:"max_days"`
	MaxBackups int    `yaml:"max_backups"`
}

type Security struct {
	SSLCA           string   `toml:"ssl-ca" json:"ssl-ca"`
	SSLCert         string   `toml:"ssl-cert" json:"ssl-cert"`
	SSLKey          string   `toml:"ssl-key" json:"ssl-key"`
	ClusterSSLCA    string   `toml:"cluster-ssl-ca" json:"cluster-ssl-ca"`
	ClusterSSLCert  string   `toml:"cluster-ssl-cert" json:"cluster-ssl-cert"`
	ClusterSSLKey   string   `toml:"cluster-ssl-key" json:"cluster-ssl-key"`
	ClusterVerifyCN []string `toml:"cluster-verify-cn" json:"cluster-verify-cn"`
	MinTLSVersion   string   `toml:"tls-version" json:"tls-version"`
	RSAKeySize      int      `toml:"rsa-key-size" json:"rsa-key-size"`
}

func NewProxyConfig(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.Check(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (cfg *Config) Check() error {
	if cfg.Metrics.PromCluster == "" {
		cfg.Metrics.PromCluster = DefaultClusterName
	}
	if cfg.Workdir == "" {
		d, err := os.Getwd()
		if err != nil {
			return err
		}
		cfg.Workdir = filepath.Clean(d)
	}
	return nil
}

func (cfg *Config) ToBytes() ([]byte, error) {
	return yaml.Marshal(cfg)
}
