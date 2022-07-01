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
	"io/ioutil"

	"github.com/goccy/go-yaml"
)

type Namespace struct {
	Version     string            `yaml:"version"`
	Namespace   string            `yaml:"namespace"`
	Frontend    FrontendNamespace `yaml:"frontend"`
	Backend     BackendNamespace  `yaml:"backend"`
	Breaker     BreakerInfo       `yaml:"breaker"`
	RateLimiter RateLimiterInfo   `yaml:"rate_limiter"`
}

type FrontendNamespace struct {
	AllowedDBs   []string  `yaml:"allowed_dbs"`
	SlowSQLTime  int       `yaml:"slow_sql_time"`
	DeniedIPs    []string  `yaml:"denied_ips"`
	Usernames    []string  `yaml:"usernames"`
	SQLBlackList []SQLInfo `yaml:"sql_blacklist"`
	SQLWhiteList []SQLInfo `yaml:"sql_whitelist"`
}

type SQLInfo struct {
	SQL string `yaml:"sql"`
}

type RateLimiterInfo struct {
	Scope string `yaml:"scope"`
	QPS   int    `yaml:"qps"`
}

type BackendNamespace struct {
	Instances    []string `yaml:"instances"`
	SelectorType string   `yaml:"selector_type"`
}

type StrategyInfo struct {
	MinQps               int64 `yaml:"min_qps"`
	SqlTimeoutMs         int64 `yaml:"sql_timeout_ms"`
	FailureRatethreshold int64 `yaml:"failure_rate_threshold"`
	FailureNum           int64 `yaml:"failure_num"`
	OpenStatusDurationMs int64 `yaml:"open_status_duration_ms"`
	Size                 int64 `yaml:"size"`
	CellIntervalMs       int64 `yaml:"cell_interval_ms"`
}

type BreakerInfo struct {
	Scope      string         `yaml:"scope"`
	Strategies []StrategyInfo `yaml:"strategies"`
}

func NewNamespaceConfig(data []byte) (*Namespace, error) {
	var cfg Namespace
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func NewNamespaceConfigFile(path string) (*Namespace, error) {
	fileData, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return NewNamespaceConfig(fileData)
}

func (cfg *Namespace) ToBytes() ([]byte, error) {
	return yaml.Marshal(cfg)
}
