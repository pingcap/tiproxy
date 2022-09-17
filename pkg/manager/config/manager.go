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

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/mvcc"
	"go.uber.org/zap"
)

const (
	DefaultEtcdDialTimeout = 3 * time.Second
	DefaultEtcdPath        = "/config"

	PathPrefixNamespace = "ns"
	PathPrefixProxy     = "proxy"
)

var (
	ErrNoOrMultiResults = errors.Errorf("has no results or multiple results")
)

type ConfigManager struct {
	wg       waitgroup.WaitGroup
	cancel   context.CancelFunc
	logger   *zap.Logger
	kv       mvcc.WatchableKV
	basePath string

	// config
	ignoreWrongNamespace bool
	watchInterval        time.Duration

	chProxy chan *config.ProxyServerOnline
}

func NewConfigManager() *ConfigManager {
	return &ConfigManager{
		chProxy: make(chan *config.ProxyServerOnline, 1),
	}
}

func (srv *ConfigManager) Init(ctx context.Context, kv mvcc.WatchableKV, cfg *config.Config, logger *zap.Logger) error {
	srv.logger = logger
	srv.ignoreWrongNamespace = cfg.Advance.IgnoreWrongNamespace
	// slash appended to distinguish '/dir'(file) and '/dir/'(directory)
	srv.basePath = appendSlashToDirPath(DefaultEtcdPath)

	srv.kv = kv

	ctx, cancel := context.WithCancel(ctx)
	srv.cancel = cancel

	return srv.watchCfgProxy(ctx, cfg)
}

func (e *ConfigManager) watch(ctx context.Context, ns, key string, f func(*zap.Logger, mvccpb.Event)) {
	wkey := []byte(path.Join(e.basePath, ns, key))
	logger := e.logger.With(zap.String("component", string(wkey)))
	retryInterval := 5 * time.Second
	e.wg.Run(func() {
		wch := e.kv.NewWatchStream()
		defer wch.Close()
		for {
			if _, err := wch.Watch(mvcc.AutoWatchID, wkey, getPrefix(wkey), wch.Rev()); err == nil {
				break
			}
			if k := retryInterval * 2; k < e.watchInterval {
				retryInterval = k
			}
			logger.Warn("failed to watch, will try again later", zap.Duration("sleep", retryInterval))
			select {
			case <-ctx.Done():
				return
			case <-time.After(retryInterval):
			}
		}
		for {
			select {
			case <-ctx.Done():
				return
			case res := <-wch.Chan():
				for _, evt := range res.Events {
					f(logger, evt)
				}
			}
		}
	})
}

func (e *ConfigManager) get(ctx context.Context, ns, key string) (*mvccpb.KeyValue, error) {
	resp, err := e.kv.Range(ctx, []byte(path.Join(e.basePath, ns, key)), nil, mvcc.RangeOptions{Rev: 0})
	if err != nil {
		return nil, err
	}
	if len(resp.KVs) != 1 {
		return nil, ErrNoOrMultiResults
	}
	return &resp.KVs[0], nil
}

func getPrefix(key []byte) []byte {
	end := make([]byte, len(key))
	copy(end, key)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			end = end[:i+1]
			return end
		}
	}
	return []byte{0}
}

func (e *ConfigManager) list(ctx context.Context, ns string, ops ...clientv3.OpOption) ([]mvccpb.KeyValue, error) {
	k := []byte(path.Join(e.basePath, ns))
	resp, err := e.kv.Range(ctx, k, getPrefix(k), mvcc.RangeOptions{Rev: 0})
	if err != nil {
		return nil, err
	}
	return resp.KVs, nil
}

func (e *ConfigManager) set(ctx context.Context, ns, key, val string) error {
	_ = e.kv.Put([]byte(path.Join(e.basePath, ns, key)), []byte(val), lease.NoLease)
	return nil
}

func (e *ConfigManager) del(ctx context.Context, ns, key string) error {
	_, _ = e.kv.DeleteRange([]byte(path.Join(e.basePath, ns, key)), nil)
	return nil
}

func (e *ConfigManager) Close() error {
	e.cancel()
	e.wg.Wait()
	return nil
}
