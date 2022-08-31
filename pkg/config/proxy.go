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
	Workdir string `yaml:"workdir" toml:"workdir" json:"workdir"`

	LCUrlsI []string  `yaml:"listen-urls" toml:"listen-urls" json:"listen-urls"`
	ACUrlsI []string  `yaml:"advertise-urls" toml:"advertise-urls" json:"advertise-urls"`
	LPUrlsI []string  `yaml:"listen-peer-urls" toml:"listen-peer-urls" json:"listen-peer-urls"`
	APUrlsI []string  `yaml:"advertise-peer-urls" toml:"advertise-peer-urls" json:"advertise-peer-urls"`
	LCUrls  []url.URL `yaml:"-" toml:"-" json:"-"`
	ACUrls  []url.URL `yaml:"-" toml:"-" json:"-"`
	LPUrls  []url.URL `yaml:"-" toml:"-" json:"-"`
	APUrls  []url.URL `yaml:"-" toml:"-" json:"-"`

	Config   ConfigManager `yaml:"config" toml:"config" json:"config"`
	Proxy    ProxyServer   `yaml:"proxy" toml:"proxy" json:"proxy"`
	API      API           `yaml:"api" toml:"api" json:"api"`
	Metrics  Metrics       `yaml:"metrics" toml:"metrics" json:"metrics"`
	Log      Log           `yaml:"log" toml:"log" json:"log"`
	Security Security      `yaml:"security" toml:"security" json:"security"`
}

type Metrics struct {
	PromCluster string `yaml:"prom-cluster" toml:"prom-cluster" json:"prom-cluster"`
}

type ConfigManager struct {
	IgnoreWrongNamespace bool   `yaml:"ignore-wrong-namespace" toml:"ignore-wrong-namespace" json:"ignore-wrong-namespace"`
	WatchInterval        string `yaml:"watch-interval" toml:"watch-interval" json:"watch-interval"`
}

type ProxyServerOnline struct {
	MaxConnections uint64 `yaml:"max-connections" toml:"max-connections" json:"max-connections"`
	TCPKeepAlive   bool   `yaml:"tcp-keep-alive" toml:"tcp-keep-alive" json:"tcp-keep-alive"`
}

type ProxyServer struct {
	ProxyServerOnline
	Addr          string `yaml:"addr" toml:"addr" json:"addr"`
	PDAddrs       string `yaml:"pd-addrs" toml:"pd-addrs" json:"pd-addrs"`
	ProxyProtocol string `yaml:"proxy-protocol" toml:"proxy-protocol" json:"proxy-protocol"`
}

type API struct {
	EnableBasicAuth bool   `yaml:"enable-basic-auth" toml:"enable-basic-auth" json:"enable-basic-auth"`
	User            string `yaml:"user" toml:"user" json:"user"`
	Password        string `yaml:"password" toml:"password" json:"password"`
}

type Log struct {
	Level   string  `yaml:"level" toml:"level" json:"level"`
	Encoder string  `yaml:"encoder" toml:"encoder" json:"encoder"`
	LogFile LogFile `yaml:"log-file" toml:"log-file" json:"log-file"`
}

type LogFile struct {
	Filename   string `yaml:"filename" toml:"filename" json:"filename"`
	MaxSize    int    `yaml:"max-size" toml:"max-size" json:"max-size"`
	MaxDays    int    `yaml:"max-days" toml:"max-days" json:"max-days"`
	MaxBackups int    `yaml:"max-backups" toml:"max-backups" json:"max-backups"`
}

type TLSCert struct {
	CA   string `yaml:"ca" toml:"ca" json:"ca"`
	Cert string `yaml:"cert" toml:"cert" json:"cert"`
	Key  string `yaml:"key" toml:"key" json:"key"`
}

func (c TLSCert) HasCert() bool {
	return !(c.Cert == "" && c.Key == "")
}

func (c TLSCert) HasCA() bool {
	return c.CA != ""
}

type Security struct {
	RSAKeySize int     `yaml:"rsa-key-size" toml:"rsa-key-size" json:"rsa-key-size"`
	Server     TLSCert `yaml:"server" toml:"server" json:"server"`
	Cluster    TLSCert `yaml:"cluster" toml:"cluster" json:"cluster"`
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
