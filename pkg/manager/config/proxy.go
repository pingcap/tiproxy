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

	"github.com/pingcap/TiProxy/pkg/config"
	"github.com/pingcap/TiProxy/pkg/util/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	ErrZeroMaxConn = errors.New("zero max connection set")
)

func (e *ConfigManager) initProxyConfig(ctx context.Context) {
	e.watch(ctx, PathPrefixProxy, "config", func(logger *zap.Logger, evt *clientv3.Event) {
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
	_, err = e.set(ctx, PathPrefixProxy, "config", string(value))
	return err
}
