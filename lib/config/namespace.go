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
	Instances    []string  `yaml:"instances" json:"instances" toml:"instances"`
	SelectorType string    `yaml:"selector-type" json:"selector-type" toml:"selector-type"`
	Security     TLSConfig `yaml:"security" json:"security" toml:"security"`
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
