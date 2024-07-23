// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"

	"github.com/BurntSushi/toml"
)

type Namespace struct {
	Namespace string            `yaml:"namespace" json:"namespace" toml:"namespace"`
	Frontend  FrontendNamespace `yaml:"frontend" json:"frontend" toml:"frontend"`
	Backend   BackendNamespace  `yaml:"backend" json:"backend" toml:"backend"`
}

type FrontendNamespace struct {
	User     string    `yaml:"user" json:"user" toml:"user"`
	Security TLSConfig `yaml:"security" json:"security" toml:"security"`
}

type BackendNamespace struct {
	Instances []string  `yaml:"instances" json:"instances" toml:"instances"`
	Security  TLSConfig `yaml:"security" json:"security" toml:"security"`
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
