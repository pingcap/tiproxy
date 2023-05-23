// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"
	"time"

	"github.com/BurntSushi/toml"
)

type Namespace struct {
	Name     string            `yaml:"name" json:"name" toml:"name"`
	Frontend FrontendNamespace `yaml:"frontend" json:"frontend" toml:"frontend"`
	Backend  BackendNamespace  `yaml:"backend" json:"backend" toml:"backend"`
}

type FrontendNamespace struct {
	User     string    `yaml:"user" json:"user" toml:"user"`
	Security TLSConfig `yaml:"security" json:"security" toml:"security"`
}

type BackendNamespace struct {
	Instances    []string  `yaml:"instances" json:"instances" toml:"instances"`
	SelectorType string    `yaml:"selector-type" json:"selector-type" toml:"selector-type"`
	Security     TLSConfig `yaml:"security" json:"security" toml:"security"`
	//HealthCheck  HealthCheck `yaml:"health-check" json:"health-check" toml:"health-check"`
}

const (
	healthCheckInterval      = 3 * time.Second
	healthCheckMaxRetries    = 3
	healthCheckRetryInterval = 1 * time.Second
	healthCheckTimeout       = 2 * time.Second
	tombstoneThreshold       = 5 * time.Minute
)

// HealthCheck contains some configurations for health check.
// Some general configurations of them may be exposed to users in the future.
// We can use shorter durations to speed up unit tests.
type HealthCheck struct {
	Enable             bool          `yaml:"enable" json:"enable" toml:"enable"`
	Interval           time.Duration `yaml:"interval" json:"interval" toml:"interval"`
	MaxRetries         int           `yaml:"max-retries" json:"max-retries" toml:"max-retries"`
	RetryInterval      time.Duration `yaml:"retry-interval" json:"retry-interval" toml:"retry-interval"`
	DialTimeout        time.Duration `yaml:"dial-timeout" json:"dial-timeout" toml:"dial-timeout"`
	TombstoneThreshold time.Duration `yaml:"tombstone-threshold" json:"tombstone-threshold" toml:"tombstone-threshold"`
}

// NewDefaultHealthCheckConfig creates a default HealthCheck.
func NewDefaultHealthCheckConfig() *HealthCheck {
	return &HealthCheck{
		Enable:             true,
		Interval:           healthCheckInterval,
		MaxRetries:         healthCheckMaxRetries,
		RetryInterval:      healthCheckRetryInterval,
		DialTimeout:        healthCheckTimeout,
		TombstoneThreshold: tombstoneThreshold,
	}
}

func (hc *HealthCheck) Check() {
	if hc.Interval == 0 {
		hc.Interval = healthCheckInterval
	}
	if hc.MaxRetries == 0 {
		hc.MaxRetries = healthCheckMaxRetries
	}
	if hc.RetryInterval == 0 {
		hc.RetryInterval = healthCheckRetryInterval
	}
	if hc.DialTimeout == 0 {
		hc.DialTimeout = healthCheckTimeout
	}
	if hc.TombstoneThreshold == 0 {
		hc.TombstoneThreshold = tombstoneThreshold
	}
}

func NewNamespace(data []byte) (*Namespace, error) {
	var cfg Namespace
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (cfg *Namespace) ToBytes() ([]byte, error) {
	b := new(bytes.Buffer)
	err := toml.NewEncoder(b).Encode(cfg)
	return b.Bytes(), err
}
