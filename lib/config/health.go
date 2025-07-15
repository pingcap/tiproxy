// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import "time"

type HealthInfo struct {
	ConfigChecksum uint32 `json:"config_checksum"`
}

const (
	healthCheckInterval      = 3 * time.Second
	healthCheckMaxRetries    = 3
	healthCheckRetryInterval = 1 * time.Second
	healthCheckTimeout       = 2 * time.Second
	readMetricsInterval      = 5 * time.Second
	readMetricsTimeout       = 3 * time.Second
)

// HealthCheck contains some configurations for health check.
// Some general configurations of them may be exposed to users in the future.
// We can use shorter durations to speed up unit tests.
type HealthCheck struct {
	Enable          bool          `yaml:"enable" json:"enable" toml:"enable" reloadable:"false"`
	Interval        time.Duration `yaml:"interval" json:"interval" toml:"interval" reloadable:"false"`
	MaxRetries      int           `yaml:"max-retries" json:"max-retries" toml:"max-retries" reloadable:"false"`
	RetryInterval   time.Duration `yaml:"retry-interval" json:"retry-interval" toml:"retry-interval" reloadable:"false"`
	DialTimeout     time.Duration `yaml:"dial-timeout" json:"dial-timeout" toml:"dial-timeout" reloadable:"false"`
	MetricsInterval time.Duration `yaml:"metrics-interval" json:"metrics-interval" toml:"metrics-interval" reloadable:"false"`
	MetricsTimeout  time.Duration `yaml:"metrics-timeout" json:"metrics-timeout" toml:"metrics-timeout" reloadable:"false"`
}

// NewDefaultHealthCheckConfig creates a default HealthCheck.
func NewDefaultHealthCheckConfig() *HealthCheck {
	return &HealthCheck{
		Enable:          true,
		Interval:        healthCheckInterval,
		MaxRetries:      healthCheckMaxRetries,
		RetryInterval:   healthCheckRetryInterval,
		DialTimeout:     healthCheckTimeout,
		MetricsInterval: readMetricsInterval,
		MetricsTimeout:  readMetricsTimeout,
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
	if hc.MetricsInterval == 0 {
		hc.MetricsInterval = readMetricsInterval
	}
	if hc.MetricsTimeout == 0 {
		hc.MetricsTimeout = readMetricsTimeout
	}
}
