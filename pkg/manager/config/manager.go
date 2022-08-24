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
	"path"
	"time"

	"github.com/pingcap/TiProxy/pkg/config"
	"github.com/pingcap/TiProxy/pkg/util/errors"
	"github.com/pingcap/TiProxy/pkg/util/waitgroup"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	DefaultEtcdDialTimeout = 3 * time.Second
	DefaultWatchInterval   = 10 * time.Minute
	DefaultEtcdPath        = "/config"

	PathPrefixNamespace = "ns"
	PathPrefixProxy     = "proxy"
)

var (
	ErrNoOrMultiResults = errors.Errorf("has no results or multiple results")
)

type ConfigManager struct {
	wg         waitgroup.WaitGroup
	cancel     context.CancelFunc
	logger     *zap.Logger
	etcdClient *clientv3.Client
	kv         clientv3.KV
	basePath   string

	// config
	IgnoreWrongNamespace bool
	WatchInterval        time.Duration

	chProxy chan *config.ProxyServerOnline
}

func NewConfigManager() *ConfigManager {
	return &ConfigManager{
		chProxy: make(chan *config.ProxyServerOnline, 1),
	}
}

func (srv *ConfigManager) Init(ctx context.Context, addrs []string, cfg config.ConfigManager, logger *zap.Logger) error {
	srv.logger = logger
	srv.IgnoreWrongNamespace = cfg.IgnoreWrongNamespace
	if cfg.WatchInterval == "" {
		srv.WatchInterval = DefaultWatchInterval
	} else {
		wi, err := time.ParseDuration(cfg.WatchInterval)
		if err != nil {
			return errors.Wrapf(err, "failed to parser watch interval %s", cfg.WatchInterval)
		}
		srv.WatchInterval = wi
	}
	// slash appended to distinguish '/dir'(file) and '/dir/'(directory)
	srv.basePath = appendSlashToDirPath(DefaultEtcdPath)

	etcdConfig := clientv3.Config{
		Endpoints:   addrs,
		DialTimeout: DefaultEtcdDialTimeout,
	}

	etcdClient, err := clientv3.New(etcdConfig)
	if err != nil {
		return errors.Wrapf(err, "create etcd config center error")
	}
	srv.etcdClient = etcdClient
	srv.kv = clientv3.NewKV(srv.etcdClient)

	ctx, cancel := context.WithCancel(ctx)
	srv.cancel = cancel

	srv.initProxyConfig(ctx)

	return nil
}

func (e *ConfigManager) watch(ctx context.Context, ns, key string, f func(*zap.Logger, *clientv3.Event)) {
	wkey := path.Join(e.basePath, ns, key)
	logger := e.logger.With(zap.String("component", wkey))
	e.wg.Run(func() {
		var prevKV *mvccpb.KeyValue

		ticker := time.NewTicker(e.WatchInterval)
		defer ticker.Stop()

		wch := e.etcdClient.Watch(ctx, wkey)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				resp, err := e.kv.Get(ctx, wkey)
				if err != nil {
					logger.Warn("failed to poll", zap.Error(err))
					break
				}
				// len == 0 may mean there is no value set yet, do not warn about that
				if len(resp.Kvs) > 1 {
					logger.Warn("failed to poll", zap.Error(ErrNoOrMultiResults))
					break
				} else if len(resp.Kvs) == 1 {
					f(logger, &clientv3.Event{
						Type:   mvccpb.PUT,
						Kv:     resp.Kvs[0],
						PrevKv: prevKV,
					})
					prevKV = resp.Kvs[0]
				}
			case res := <-wch:
				if res.Canceled {
					logger.Warn("failed to watch, try again", zap.Error(res.Err()))
					wch = e.etcdClient.Watch(ctx, wkey)
					break
				}

				for _, evt := range res.Events {
					f(logger, evt)
					prevKV = evt.Kv
				}

				// reset the ticker to prevent another tick immediately
				ticker.Reset(e.WatchInterval)
			}
		}
	})
}

func (e *ConfigManager) get(ctx context.Context, ns, key string) (*mvccpb.KeyValue, error) {
	resp, err := e.kv.Get(ctx, path.Join(e.basePath, ns, key))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) != 1 {
		return nil, ErrNoOrMultiResults
	}
	return resp.Kvs[0], nil
}

func (e *ConfigManager) list(ctx context.Context, ns string, ops ...clientv3.OpOption) ([]*mvccpb.KeyValue, error) {
	options := make([]clientv3.OpOption, 1, 1+len(ops))
	options[0] = clientv3.WithPrefix()
	options = append(options, ops...)
	resp, err := e.kv.Get(ctx, path.Join(e.basePath, ns), options...)
	if err != nil {
		return nil, err
	}
	return resp.Kvs, nil
}

func (e *ConfigManager) set(ctx context.Context, ns, key, val string) (*mvccpb.KeyValue, error) {
	resp, err := e.kv.Put(ctx, path.Join(e.basePath, ns, key), val)
	if err != nil {
		return nil, err
	}
	return resp.PrevKv, nil
}

func (e *ConfigManager) del(ctx context.Context, ns, key string) error {
	_, err := e.kv.Delete(ctx, path.Join(e.basePath, ns, key))
	if err != nil {
		return err
	}
	return nil
}

func (e *ConfigManager) Close() error {
	e.cancel()
	e.wg.Wait()
	return errors.Wrapf(e.etcdClient.Close(), "fail to close config manager")
}
