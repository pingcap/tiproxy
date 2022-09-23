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
	"github.com/pingcap/TiProxy/lib/util/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"
)

type CfgType int

const (
	CfgServer CfgType = iota
	CfgLog
)

type OnlineCfgTypes interface {
	config.ProxyServerOnline | config.LogOnline
}

type imeta interface {
	getPrefix() string
	unmarshal(bytes []byte) (any, error)
	addToCh(any)
	getInitial(cfg *config.Config) any
}

type meta[T OnlineCfgTypes] struct {
	prefix   string
	initFunc func(cfg *config.Config) T
	ch       chan *T
}

func newMeta[T OnlineCfgTypes](prefix string, initFunc func(cfg *config.Config) T) *meta[T] {
	return &meta[T]{
		prefix:   prefix,
		initFunc: initFunc,
		ch:       make(chan *T, 1),
	}
}

func (m meta[T]) unmarshal(bytes []byte) (any, error) {
	var t T
	err := json.Unmarshal(bytes, &t)
	return &t, err
}

func (m meta[T]) getPrefix() string {
	return m.prefix
}

func (m meta[T]) addToCh(obj any) {
	m.ch <- obj.(*T)
}

func (m meta[T]) getInitial(cfg *config.Config) any {
	return m.initFunc(cfg)
}

func (e *ConfigManager) initMetas() {
	e.metas = map[CfgType]imeta{
		CfgServer: newMeta(pathPrefixProxyServer, func(cfg *config.Config) config.ProxyServerOnline {
			return cfg.Proxy.ProxyServerOnline
		}),
		CfgLog: newMeta(pathPrefixLog, func(cfg *config.Config) config.LogOnline {
			return cfg.Log.LogOnline
		}),
	}
}

func (e *ConfigManager) watchCfgProxy(ctx context.Context, cfg *config.Config) error {
	for _, m := range e.metas {
		if err := func(m imeta) error {
			_, err := e.get(ctx, m.getPrefix(), "config")
			if err != nil && errors.Is(err, ErrNoOrMultiResults) {
				value, err := json.Marshal(m.getInitial(cfg))
				if err != nil {
					return err
				}
				if err = e.set(ctx, m.getPrefix(), "config", string(value)); err != nil {
					return err
				}
			}
			e.watch(ctx, m.getPrefix(), "config", func(logger *zap.Logger, evt mvccpb.Event) {
				if obj, err := m.unmarshal(evt.Kv.Value); err != nil {
					logger.Warn("failed unmarshal proxy config", zap.Error(err))
					return
				} else {
					m.addToCh(obj)
				}
			})
			return nil
		}(m); err != nil {
			return err
		}
	}
	return nil
}

func (e *ConfigManager) getCfg(ctx context.Context, tp CfgType) (any, error) {
	m := e.metas[tp]
	val, err := e.get(ctx, m.getPrefix(), "config")
	if err != nil {
		return nil, err
	}
	return m.unmarshal(val.Value)
}

func (e *ConfigManager) setCfg(ctx context.Context, tp CfgType, obj any) error {
	m := e.metas[tp]
	value, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return e.set(ctx, m.getPrefix(), "config", string(value))
}

// GetConfig queries the configuration from the config center.
func GetConfig[T OnlineCfgTypes](ctx context.Context, e *ConfigManager, tp CfgType) (*T, error) {
	obj, err := e.getCfg(ctx, tp)
	if err != nil {
		return nil, err
	}
	return obj.(*T), nil
}

// SetConfig sets a configuration to the config center.
func SetConfig[T OnlineCfgTypes](ctx context.Context, e *ConfigManager, tp CfgType, c *T) error {
	return e.setCfg(ctx, tp, c)
}

// GetCfgWatch returns the channel that contains updated configuration.
func GetCfgWatch[T OnlineCfgTypes](e *ConfigManager, tp CfgType) chan *T {
	mt := e.metas[tp].(*meta[T])
	return mt.ch
}

func (e *ConfigManager) GetProxyConfigWatch() <-chan *config.ProxyServerOnline {
	return GetCfgWatch[config.ProxyServerOnline](e, CfgServer)
}

func (e *ConfigManager) GetProxyConfig(ctx context.Context) (*config.ProxyServerOnline, error) {
	return GetConfig[config.ProxyServerOnline](ctx, e, CfgServer)
}

func (e *ConfigManager) SetProxyConfig(ctx context.Context, proxy *config.ProxyServerOnline) error {
	return e.setCfg(ctx, CfgServer, proxy)
}

func (e *ConfigManager) GetLogConfigWatch() <-chan *config.LogOnline {
	return GetCfgWatch[config.LogOnline](e, CfgLog)
}

func (e *ConfigManager) GetLogConfig(ctx context.Context) (*config.LogOnline, error) {
	return GetConfig[config.LogOnline](ctx, e, CfgLog)
}

func (e *ConfigManager) SetLogConfig(ctx context.Context, log *config.LogOnline) error {
	return e.setCfg(ctx, CfgLog, log)
}
