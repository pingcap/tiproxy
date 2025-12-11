// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import "github.com/pingcap/tiproxy/lib/util/errors"

const (
	BalancePolicyResource   = "resource"
	BalancePolicyLocation   = "location"
	BalancePolicyConnection = "connection"

	RoutePolicyPreferIdle = "prefer-idle"
	RoutePolicyRandom     = "random"
)

type Balance struct {
	LabelName   string `yaml:"label-name,omitempty" toml:"label-name,omitempty" json:"label-name,omitempty" reloadable:"true"`
	RoutingRule string `yaml:"routing-rule,omitempty" toml:"routing-rule,omitempty" json:"routing-rule,omitempty" reloadable:"false"`
	Policy      string `yaml:"policy,omitempty" toml:"policy,omitempty" json:"policy,omitempty" reloadable:"true"`
	RoutePolicy string `yaml:"route-policy,omitempty" toml:"route-policy,omitempty" json:"route-policy,omitempty" reloadable:"true"`
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
	switch b.RoutePolicy {
	case RoutePolicyPreferIdle, RoutePolicyRandom:
		return nil
	case "":
		b.RoutePolicy = RoutePolicyPreferIdle
	default:
		return errors.Wrapf(ErrInvalidConfigValue, "invalid balance.route-policy")
	}
	return nil
}

func DefaultBalance() Balance {
	return Balance{
		Policy:      BalancePolicyResource,
		RoutePolicy: RoutePolicyPreferIdle,
	}
}
