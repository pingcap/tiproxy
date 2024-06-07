// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

type Balance struct {
	Label    LabelBalance    `yaml:"label,omitempty" toml:"label,omitempty" json:"label,omitempty"`
	Error    ErrorBalance    `yaml:"error,omitempty" toml:"error,omitempty" json:"error,omitempty"`
	Memory   MemoryBalance   `yaml:"memory,omitempty" toml:"memory,omitempty" json:"memory,omitempty"`
	CPU      CPUBalance      `yaml:"cpu,omitempty" toml:"cpu,omitempty" json:"cpu,omitempty"`
	Location LocationBalance `yaml:"location,omitempty" toml:"location,omitempty" json:"location,omitempty"`
}

type LabelBalance struct {
	Enable    bool   `yaml:"enable,omitempty" toml:"enable,omitempty" json:"enable,omitempty"`
	LabelName string `yaml:"label-name,omitempty" toml:"label-name,omitempty" json:"label-name,omitempty"`
}

type ErrorBalance struct {
	Enable bool `yaml:"enable,omitempty" toml:"enable,omitempty" json:"enable,omitempty"`
}

type MemoryBalance struct {
	Enable bool `yaml:"enable,omitempty" toml:"enable,omitempty" json:"enable,omitempty"`
}

type CPUBalance struct {
	Enable bool `yaml:"enable,omitempty" toml:"enable,omitempty" json:"enable,omitempty"`
}

type LocationBalance struct {
	Enable        bool `yaml:"enable,omitempty" toml:"enable,omitempty" json:"enable,omitempty"`
	LocationFirst bool `yaml:"location-first,omitempty" toml:"location-first,omitempty" json:"location-first,omitempty"`
}

func DefaultBalance() Balance {
	return Balance{
		Label:    LabelBalance{Enable: false},
		Error:    ErrorBalance{Enable: true},
		Memory:   MemoryBalance{Enable: true},
		CPU:      CPUBalance{Enable: true},
		Location: LocationBalance{Enable: true, LocationFirst: true},
	}
}
