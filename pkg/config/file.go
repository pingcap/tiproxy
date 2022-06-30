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
	"path"
	"path/filepath"

	"github.com/pingcap/errors"
)

var (
	ErrNamespaceNotFound = errors.New("namespace not found")
)

// NamespaceDir is only for test use,
// please do not use it in production environment.
type NamespaceDir struct {
	dir    string
	cfgs   map[string]*Namespace // key: namespace
	nspath map[string]string     // key: namespace, value: config file path
}

func NewNamespacesFromDir(nsdir string) (*NamespaceDir, error) {
	c := &NamespaceDir{
		dir:    nsdir,
		cfgs:   make(map[string]*Namespace),
		nspath: make(map[string]string),
	}

	yamlFiles, err := listAllYamlFiles(c.dir)
	if err != nil {
		return nil, err
	}

	for _, yamlFile := range yamlFiles {
		fileData, err := ioutil.ReadFile(yamlFile)
		if err != nil {
			return nil, err
		}
		cfg, err := NewNamespaceConfig(fileData)
		if err != nil {
			return nil, err
		}
		c.cfgs[cfg.Namespace] = cfg
		c.nspath[cfg.Namespace] = yamlFile
	}

	return c, nil
}

func listAllYamlFiles(dir string) ([]string, error) {
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var ret []string
	for _, info := range infos {
		fileName := info.Name()
		if path.Ext(fileName) == ".yaml" {
			ret = append(ret, filepath.Join(dir, fileName))
		}
	}

	return ret, nil
}

func (f *NamespaceDir) GetNamespace(ns string) (*Namespace, error) {
	cfg, ok := f.cfgs[ns]
	if !ok {
		return nil, ErrNamespaceNotFound
	}
	return cfg, nil
}

func (f *NamespaceDir) ListAllNamespace() ([]*Namespace, error) {
	var ret []*Namespace
	for _, cfg := range f.cfgs {
		ret = append(ret, cfg)
	}
	return ret, nil
}
