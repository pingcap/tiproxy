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
)

type Balance struct {
	LabelName     string `yaml:"label-name,omitempty" toml:"label-name,omitempty" json:"label-name,omitempty" reloadable:"true"`
	RoutingRule   string `yaml:"routing-rule,omitempty" toml:"routing-rule,omitempty" json:"routing-rule,omitempty" reloadable:"false"`
	Policy        string `yaml:"policy,omitempty" toml:"policy,omitempty" json:"policy,omitempty" reloadable:"true"`
	RoutingPolicy string `yaml:"routing-policy,omitempty" toml:"routing-policy,omitempty" json:"routing-policy,omitempty" reloadable:"true"`
	Status        Factor `yaml:"status,omitempty" toml:"status,omitempty" json:"status,omitempty" reloadable:"true"`
	Health        Factor `yaml:"health,omitempty" toml:"health,omitempty" json:"health,omitempty" reloadable:"true"`
	Memory        Factor `yaml:"memory,omitempty" toml:"memory,omitempty" json:"memory,omitempty" reloadable:"true"`
	CPU           Factor `yaml:"cpu,omitempty" toml:"cpu,omitempty" json:"cpu,omitempty" reloadable:"true"`
	Location      Factor `yaml:"location,omitempty" toml:"location,omitempty" json:"location,omitempty" reloadable:"true"`
	ConnCount     Factor `yaml:"conn-count,omitempty" toml:"conn-count,omitempty" json:"conn-count,omitempty" reloadable:"true"`
}

type Factor struct {
	MigrationsPerSecond float64 `yaml:"migrations-per-second,omitempty" toml:"migrations-per-second,omitempty" json:"migrations-per-second,omitempty" reloadable:"true"`
}

func (b *Balance) Check() error {
	switch b.Policy {
	case BalancePolicyResource, BalancePolicyLocation, BalancePolicyConnection:
		return nil
	case "":
		b.Policy = BalancePolicyResource
	default:
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.policy")
	}
	switch b.RoutingPolicy {
	case RoutingPolicyPreferIdle, RoutingPolicyRandom:
		return nil
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
	return nil
}

func DefaultBalance() Balance {
	return Balance{
		Policy:        BalancePolicyResource,
		RoutingPolicy: RoutingPolicyPreferIdle,
	}
}
