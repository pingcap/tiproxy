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
	Workdir string `yaml:"workdir"`

	LCUrlsI []string  `yaml:"listen-urls"`
	ACUrlsI []string  `yaml:"advertise-urls"`
	LPUrlsI []string  `yaml:"listen-peer-urls"`
	APUrlsI []string  `yaml:"advertise-peer-urls"`
	LCUrls  []url.URL `yaml:"-"`
	ACUrls  []url.URL `yaml:"-"`
	LPUrls  []url.URL `yaml:"-"`
	APUrls  []url.URL `yaml:"-"`

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
	MaxConnections uint64 `yaml:"max_connections"`
	TCPKeepAlive   bool   `yaml:"tcp_keep_alive"`
	PDAddrs        string `yaml:"pd_addrs"`
	ProxyProtocol  string `yaml:"proxy_protocol"`
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

type TLSCert struct {
	CA   string `toml:"ca" json:"ca"`
	Cert string `toml:"cert" json:"cert"`
	Key  string `toml:"key" json:"key"`
}

func (c TLSCert) HasCert() bool {
	return !(c.Cert == "" && c.Key == "")
}

func (c TLSCert) HasCA() bool {
	return c.CA != ""
}

type Security struct {
	RSAKeySize int     `toml:"rsa-key-size" json:"rsa-key-size"`
	Server     TLSCert `toml:"server" json:"server"`
	Cluster    TLSCert `toml:"cluster" json:"cluster"`
}

func NewConfig(data []byte) (*Config, error) {
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
	var err error
	cfg.LCUrls, err = str2url(cfg.LCUrlsI)
	if err != nil {
		return err
	}
	cfg.ACUrls, err = str2url(cfg.ACUrlsI)
	if err != nil {
		return err
	}
	cfg.LPUrls, err = str2url(cfg.LPUrlsI)
	if err != nil {
		return err
	}
	cfg.APUrls, err = str2url(cfg.APUrlsI)
	if err != nil {
		return err
	}
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
	cfg.LCUrlsI = url2str(cfg.LCUrls)
	cfg.ACUrlsI = url2str(cfg.ACUrls)
	cfg.LPUrlsI = url2str(cfg.LPUrls)
	cfg.APUrlsI = url2str(cfg.APUrls)
	return yaml.Marshal(cfg)
}

func str2url(us []string) ([]url.URL, error) {
	r := make([]url.URL, len(us))
	for i, ustr := range us {
		url, err := url.Parse(ustr)
		if err != nil {
			return r, err
		}
		r[i] = *url
	}
	return r, nil
}

func url2str(us []url.URL) []string {
	r := make([]string, len(us))
	for i, u := range us {
		r[i] = u.String()
	}
	return r
}
