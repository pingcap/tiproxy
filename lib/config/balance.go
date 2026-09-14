// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import "github.com/pingcap/tiproxy/lib/util/errors"

const (
	BalancePolicyResource   = "resource"
	BalancePolicyLocation   = "location"
	BalancePolicyConnection = "connection"

	RoutingPolicyPreferIdle = "prefer-idle"
	RoutingPolicyRandom     = "random"
	RoutingPolicyIdlest     = "idlest"

	// MatchClientCIDRStr is used for MatchClientCIDR.
	MatchClientCIDRStr = "client_cidr"
	// MatchProxyCIDRStr is used for MatchProxyCIDR.
	MatchProxyCIDRStr = "proxy_cidr"
	// MatchPortStr is used for port-based routing.
	MatchPortStr = "port"
)

type Balance struct {
	LabelName     string          `yaml:"label-name,omitempty" toml:"label-name,omitempty" json:"label-name,omitempty" reloadable:"true"`
	RoutingRule   string          `yaml:"routing-rule,omitempty" toml:"routing-rule,omitempty" json:"routing-rule,omitempty" reloadable:"false"`
	Policy        string          `yaml:"policy,omitempty" toml:"policy,omitempty" json:"policy,omitempty" reloadable:"true"`
	RoutingPolicy string          `yaml:"routing-policy,omitempty" toml:"routing-policy,omitempty" json:"routing-policy,omitempty" reloadable:"true"`
	Status        StatusFactor    `yaml:"status,omitempty" toml:"status,omitempty" json:"status,omitempty" reloadable:"true"`
	Health        Factor          `yaml:"health" toml:"health" json:"health" reloadable:"true"`
	Memory        Factor          `yaml:"memory" toml:"memory" json:"memory" reloadable:"true"`
	CPU           Factor          `yaml:"cpu" toml:"cpu" json:"cpu" reloadable:"true"`
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

func (b *Balance) Check() error {
	switch b.Policy {
	case BalancePolicyResource, BalancePolicyLocation, BalancePolicyConnection:
	case "":
		b.Policy = BalancePolicyResource
	default:
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.policy")
	}

	switch b.RoutingRule {
	case MatchClientCIDRStr, MatchProxyCIDRStr, MatchPortStr, "":
	default:
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.routing-rule")
	}

	switch b.RoutingPolicy {
	case RoutingPolicyPreferIdle, RoutingPolicyRandom, RoutingPolicyIdlest:
	case "":
		b.RoutingPolicy = RoutingPolicyPreferIdle
	default:
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.route-policy")
	}

	if b.Status.MigrationsPerSecond < 0 {
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.status.migrations-per-second")
	}
	if b.Health.MigrationsPerSecond < 0 {
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.health.migrations-per-second")
	}
	if b.Memory.MigrationsPerSecond < 0 {
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.memory.migrations-per-second")
	}
	if b.CPU.MigrationsPerSecond < 0 {
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.cpu.migrations-per-second")
	}
	if b.Location.MigrationsPerSecond < 0 {
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.location.migrations-per-second")
	}
	if b.ConnCount.MigrationsPerSecond < 0 {
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.conn-count.migrations-per-second")
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
		CPU:           defaultFactor(),
		Location:      defaultFactor(),
	}
}

func defaultFactor() Factor {
	return Factor{Enabled: true}
}
