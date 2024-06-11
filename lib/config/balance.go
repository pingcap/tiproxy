// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import "github.com/pingcap/tiproxy/lib/util/errors"

const (
	BalancePolicyResource   = "resource"
	BalancePolicyLocation   = "location"
	BalancePolicyConnection = "connection"
)

type Balance struct {
	LabelName string `yaml:"label-name,omitempty" toml:"label-name,omitempty" json:"label-name,omitempty"`
	Policy    string `yaml:"policy,omitempty" toml:"policy,omitempty" json:"policy,omitempty"`
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
	return nil
}

func DefaultBalance() Balance {
	return Balance{
		Policy: BalancePolicyResource,
	}
}
