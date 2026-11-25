// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"math"

	"github.com/pingcap/tiproxy/lib/util/errors"
)

const (
	BalancePolicyResource   = "resource"
	BalancePolicyLocation   = "location"
	BalancePolicyConnection = "connection"

	RoutingPolicyPreferIdle = "prefer-idle"
	RoutingPolicyRandom     = "random"
	RoutingPolicyIdlest     = "idlest"
<<<<<<< HEAD
=======

	// MatchClientCIDRStr is used for MatchClientCIDR.
	MatchClientCIDRStr = "client_cidr"
	// MatchProxyCIDRStr is used for MatchProxyCIDR.
	MatchProxyCIDRStr = "proxy_cidr"
	// MatchPortStr is used for port-based routing.
	MatchPortStr = "port"

	// maxMigrationsPerSecond is one migration per nanosecond. time.Duration cannot
	// represent a shorter interval, and a larger rate truncates to 0 and then
	// panics on integer division in the rebalance loop.
	maxMigrationsPerSecond = 1e9
>>>>>>> e8e8ea2b (*: check config value bounds (#1440))
)

type Balance struct {
	LabelName     string          `yaml:"label-name,omitempty" toml:"label-name,omitempty" json:"label-name,omitempty" reloadable:"true"`
	Policy        string          `yaml:"policy,omitempty" toml:"policy,omitempty" json:"policy,omitempty" reloadable:"true"`
	RoutingPolicy string          `yaml:"routing-policy,omitempty" toml:"routing-policy,omitempty" json:"routing-policy,omitempty" reloadable:"true"`
	Status        StatusFactor    `yaml:"status,omitempty" toml:"status,omitempty" json:"status,omitempty" reloadable:"true"`
	Health        Factor          `yaml:"health" toml:"health" json:"health" reloadable:"true"`
	Memory        Factor          `yaml:"memory" toml:"memory" json:"memory" reloadable:"true"`
	CPU           CPUFactor       `yaml:"cpu" toml:"cpu" json:"cpu" reloadable:"true"`
	Location      Factor          `yaml:"location" toml:"location" json:"location" reloadable:"true"`
	ConnCount     ConnCountFactor `yaml:"conn-count,omitempty" toml:"conn-count,omitempty" json:"conn-count,omitempty" reloadable:"true"`
}

type ConnCountFactor struct {
	MigrationsPerSecond float64 `yaml:"migrations-per-second,omitempty" toml:"migrations-per-second,omitempty" json:"migrations-per-second,omitempty" reloadable:"true"`
	CountRatioThreshold float64 `yaml:"count-ratio-threshold,omitempty" toml:"count-ratio-threshold,omitempty" json:"count-ratio-threshold,omitempty" reloadable:"true"`
}

type StatusFactor struct {
	MigrationsPerSecond float64 `yaml:"migrations-per-second,omitempty" toml:"migrations-per-second,omitempty" json:"migrations-per-second,omitempty" reloadable:"true"`
}

type Factor struct {
	Enabled             bool    `yaml:"enabled" toml:"enabled" json:"enabled" reloadable:"true"`
	MigrationsPerSecond float64 `yaml:"migrations-per-second,omitempty" toml:"migrations-per-second,omitempty" json:"migrations-per-second,omitempty" reloadable:"true"`
}

type CPUFactor struct {
	Enabled             bool    `yaml:"enabled" toml:"enabled" json:"enabled" reloadable:"true"`
	MigrationsPerSecond float64 `yaml:"migrations-per-second,omitempty" toml:"migrations-per-second,omitempty" json:"migrations-per-second,omitempty" reloadable:"true"`
	MinBalanceUsage     float64 `yaml:"min-balance-usage" toml:"min-balance-usage" json:"min-balance-usage" reloadable:"true"`
	MaxUsageGap         float64 `yaml:"max-usage-gap" toml:"max-usage-gap" json:"max-usage-gap" reloadable:"true"`
}

func checkMigrationsPerSecond(name string, v float64) error {
	if math.IsNaN(v) || v < 0 || v > maxMigrationsPerSecond {
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.%s.migrations-per-second, must be between 0 and %g", name, maxMigrationsPerSecond)
	}
	return nil
}

func (b *Balance) Check() error {
	switch b.Policy {
	case BalancePolicyResource, BalancePolicyLocation, BalancePolicyConnection:
	case "":
		b.Policy = BalancePolicyResource
	default:
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.policy")
	}

	switch b.RoutingPolicy {
	case RoutingPolicyPreferIdle, RoutingPolicyRandom, RoutingPolicyIdlest:
	case "":
		b.RoutingPolicy = RoutingPolicyPreferIdle
	default:
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.route-policy")
	}

	if err := checkMigrationsPerSecond("status", b.Status.MigrationsPerSecond); err != nil {
		return err
	}
	if err := checkMigrationsPerSecond("health", b.Health.MigrationsPerSecond); err != nil {
		return err
	}
	if err := checkMigrationsPerSecond("memory", b.Memory.MigrationsPerSecond); err != nil {
		return err
	}
	if err := checkMigrationsPerSecond("cpu", b.CPU.MigrationsPerSecond); err != nil {
		return err
	}
	if b.CPU.MinBalanceUsage < 0 || b.CPU.MinBalanceUsage > 1 {
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.cpu.min-balance-usage")
	}
	if b.CPU.MaxUsageGap == 0 {
		b.CPU.MaxUsageGap = 1
	} else if b.CPU.MaxUsageGap < 0.05 || b.CPU.MaxUsageGap > 1 {
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.cpu.max-usage-gap")
	}
	if err := checkMigrationsPerSecond("location", b.Location.MigrationsPerSecond); err != nil {
		return err
	}
	if err := checkMigrationsPerSecond("conn-count", b.ConnCount.MigrationsPerSecond); err != nil {
		return err
	}
	if b.ConnCount.CountRatioThreshold != 0 && b.ConnCount.CountRatioThreshold <= 1 {
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.conn-count.count-ratio-threshold")
	}
	return nil
}

func DefaultBalance() Balance {
	return Balance{
		Policy:        BalancePolicyResource,
		RoutingPolicy: RoutingPolicyPreferIdle,
		Health:        defaultFactor(),
		Memory:        defaultFactor(),
		CPU:           CPUFactor{Enabled: true, MaxUsageGap: 1},
		Location:      defaultFactor(),
	}
}

func defaultFactor() Factor {
	return Factor{Enabled: true}
}
