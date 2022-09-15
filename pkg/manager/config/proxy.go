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

	"github.com/pingcap/TiProxy/lib/config"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"
)

func (e *ConfigManager) initProxyConfig(ctx context.Context) {
	e.watch(ctx, PathPrefixProxy, "config", func(logger *zap.Logger, evt mvccpb.Event) {
		var proxy config.ProxyServerOnline
		if err := json.Unmarshal(evt.Kv.Value, &proxy); err != nil {
			logger.Warn("failed unmarshal proxy config", zap.Error(err))
			return
		}
		e.chProxy <- &proxy
	})
}

func (e *ConfigManager) GetProxyConfig() <-chan *config.ProxyServerOnline {
	return e.chProxy
}

func (e *ConfigManager) SetProxyConfig(ctx context.Context, proxy *config.ProxyServerOnline) error {
	value, err := json.Marshal(proxy)
	if err != nil {
		return err
	}
	return e.set(ctx, PathPrefixProxy, "config", string(value))
}
