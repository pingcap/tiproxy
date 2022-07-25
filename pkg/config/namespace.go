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
	"io/ioutil"

	"github.com/goccy/go-yaml"
)

type Namespace struct {
	Namespace string            `yaml:"namespace"`
	Frontend  FrontendNamespace `yaml:"frontend"`
	Backend   BackendNamespace  `yaml:"backend"`
}

type FrontendNamespace struct {
}

type BackendNamespace struct {
	Instances    []string `yaml:"instances"`
	SelectorType string   `yaml:"selector_type"`
}

func NewNamespaceConfig(data []byte) (*Namespace, error) {
	var cfg Namespace
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func NewNamespaceConfigFile(path string) (*Namespace, error) {
	fileData, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return NewNamespaceConfig(fileData)
}

func (cfg *Namespace) ToBytes() ([]byte, error) {
	return yaml.Marshal(cfg)
}
