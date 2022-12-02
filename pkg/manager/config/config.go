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
	"reflect"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"go.uber.org/zap"
)

var cfg2Path = map[reflect.Type]string{
	reflect.TypeOf(config.ProxyServerOnline{}): "proxy",
	reflect.TypeOf(config.LogOnline{}):         "log",
}

func (e *ConfigManager) SetConfig(ctx context.Context, val any) error {
	rf := reflect.TypeOf(val)
	if rf.Kind() == reflect.Pointer {
		rf = rf.Elem()
	}
	p, ok := cfg2Path[rf]
	if !ok {
		return errors.WithStack(errors.New("invalid type"))
	}
	c, err := json.Marshal(val)
	if err != nil {
		return errors.WithStack(err)
	}
	return e.set(ctx, pathPrefixConfig, p, c)
}

func (e *ConfigManager) GetConfig(ctx context.Context, val any) error {
	rf := reflect.TypeOf(val)
	if rf.Kind() == reflect.Pointer {
		rf = rf.Elem()
	}
	p, ok := cfg2Path[rf]
	if !ok {
		return errors.WithStack(errors.New("invalid type"))
	}

	c, err := e.get(ctx, pathPrefixConfig, p)
	if err != nil {
		return err
	}

	return json.Unmarshal(c.Value, val)
}

func MakeConfigChan[T any](e *ConfigManager, initval *T) <-chan *T {
	cfgchan := make(chan *T, 64)
	rf := reflect.TypeOf(initval)
	if rf.Kind() == reflect.Pointer {
		rf = rf.Elem()
	}
	p, ok := cfg2Path[rf]
	if !ok {
		panic(errors.WithStack(errors.New("invalid type")))
	}

	_ = e.SetConfig(context.Background(), initval)

	e.Watch(path.Join(pathPrefixConfig, p), func(_ *zap.Logger, k KVEvent) {
		var v T
		if k.Type == KVEventDel {
			cfgchan <- &v
		} else {
			e := json.Unmarshal(k.Value, &v)
			if e == nil {
				cfgchan <- &v
			}
		}
	})
	return cfgchan
}
