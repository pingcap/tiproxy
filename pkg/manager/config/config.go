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
	"encoding/json"
	"path"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"go.uber.org/zap"
)

func (e *ConfigManager) SetConfig(ctx context.Context, val *config.Config) error {
	c, err := json.Marshal(val)
	if err != nil {
		return errors.WithStack(err)
	}
	return e.set(ctx, pathPrefixConfig, "all", c)
}

func (e *ConfigManager) GetConfig(ctx context.Context, val *config.Config) error {
	c, err := e.get(ctx, pathPrefixConfig, "all")
	if err != nil {
		return err
	}

	return json.Unmarshal(c.Value, val)
}

func (e *ConfigManager) WatchConfig(ctx context.Context) <-chan *config.Config {
	ch := make(chan *config.Config)
	e.Watch(path.Join(pathPrefixConfig, "all"), func(_ *zap.Logger, k KVEvent) {
		var c config.Config
		if k.Type == KVEventDel {
			ch <- &c
		} else {
			e := json.Unmarshal(k.Value, &c)
			if e == nil {
				ch <- &c
			}
		}
	})
	return ch
}
