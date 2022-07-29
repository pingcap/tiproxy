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
	"context"
	"io/ioutil"
	"path/filepath"

	"github.com/pingcap/TiProxy/pkg/config"
	"go.uber.org/zap"
)

func (e *ConfigManager) GetNamespace(ctx context.Context, ns string) (*config.Namespace, error) {
	etcdKeyValue, err := e.get(ctx, PathPrefixNamespace, ns)
	if err != nil {
		return nil, err
	}
	return config.NewNamespaceConfig(etcdKeyValue.Value)
}

func (e *ConfigManager) ListAllNamespace(ctx context.Context) ([]*config.Namespace, error) {
	etcdKeyValues, err := e.list(ctx, PathPrefixNamespace)
	if err != nil {
		return nil, err
	}

	var ret []*config.Namespace
	for _, kv := range etcdKeyValues {
		nsCfg, err := config.NewNamespaceConfig(kv.Value)
		if err != nil {
			if e.cfg.IgnoreWrongNamespace {
				e.logger.Warn("parse namespace config error", zap.Error(err), zap.ByteString("namespace", kv.Key))
				continue
			} else {
				return nil, err
			}
		}
		ret = append(ret, nsCfg)
	}

	return ret, nil
}

func (e *ConfigManager) SetNamespace(ctx context.Context, ns string, nsc *config.Namespace) error {
	r, err := nsc.ToBytes()
	if err != nil {
		return err
	}
	_, err = e.set(ctx, PathPrefixNamespace, ns, string(r))
	return err
}

func (e *ConfigManager) DelNamespace(ctx context.Context, ns string) error {
	return e.del(ctx, PathPrefixNamespace, ns)
}

func (e *ConfigManager) ImportNamespaceFromDir(ctx context.Context, dir string) error {
	yamlFiles, err := listAllYamlFiles(dir)
	if err != nil {
		return err
	}

	for _, yamlFile := range yamlFiles {
		fileData, err := ioutil.ReadFile(yamlFile)
		if err != nil {
			return err
		}
		cfg, err := config.NewNamespaceConfig(fileData)
		if err != nil {
			return err
		}
		if err := e.SetNamespace(ctx, cfg.Namespace, cfg); err != nil {
			return err
		}
	}

	return nil
}

func listAllYamlFiles(dir string) ([]string, error) {
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var ret []string
	for _, info := range infos {
		fileName := info.Name()
		if filepath.Ext(fileName) == ".yaml" {
			ret = append(ret, filepath.Join(dir, fileName))
		}
	}

	return ret, nil
}
