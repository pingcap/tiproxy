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
	"strings"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/tidwall/btree"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	pathPrefixNamespace = "ns"
	pathPrefixConfig    = "config"
)

var (
	ErrNoResults   = errors.Errorf("has no results")
	ErrFail2Update = errors.Errorf("failed to update")
)

type KVValue struct {
	Key   string
	Value []byte
}

type KVEventType byte

const (
	KVEventPut KVEventType = iota
	KVEventDel
)

type KVEvent struct {
	KVValue
	Type KVEventType
}

type kvListener struct {
	key string
	wfn func(*zap.Logger, KVEvent)
}

type ConfigManager struct {
	wg        waitgroup.WaitGroup
	cancel    context.CancelFunc
	kv        *btree.BTreeG[KVValue]
	events    chan KVEvent
	listeners []kvListener
	logger    *zap.Logger
}

func NewConfigManager() *ConfigManager {
	return &ConfigManager{}
}

func (srv *ConfigManager) Init(ctx context.Context, cfg *config.Config, logger *zap.Logger) error {
	srv.logger = logger
	srv.events = make(chan KVEvent, 256)
	srv.kv = btree.NewBTreeG(func(a, b KVValue) bool {
		return a.Key < b.Key
	})

	var nctx context.Context
	nctx, srv.cancel = context.WithCancel(ctx)
	srv.wg.Run(func() {
		for {
			select {
			case <-nctx.Done():
				return
			case ev := <-srv.events:
				for _, lsn := range srv.listeners {
					lsn.wfn(srv.logger, ev)
				}
			}
		}
	})
	return nil
}

func (e *ConfigManager) Watch(key string, handler func(*zap.Logger, KVEvent)) {
	e.listeners = append(e.listeners, kvListener{key, handler})
}

func (e *ConfigManager) get(ctx context.Context, ns, key string) (KVValue, error) {
	nkey := path.Clean(path.Join(ns, key))
	v, ok := e.kv.Get(KVValue{Key: nkey})
	if !ok {
		return v, errors.WithStack(errors.Wrapf(ErrNoResults, "key=%s", nkey))
	}
	return v, nil
}

func (e *ConfigManager) list(ctx context.Context, ns string, ops ...clientv3.OpOption) ([]KVValue, error) {
	k := path.Clean(ns)
	var resp []KVValue
	e.kv.Ascend(KVValue{Key: k}, func(item KVValue) bool {
		if !strings.HasPrefix(item.Key, k) {
			return false
		}
		resp = append(resp, item)
		return true
	})
	return resp, nil
}

func (e *ConfigManager) set(ctx context.Context, ns, key string, val []byte) error {
	v := KVValue{Key: path.Clean(path.Join(ns, key)), Value: val}
	_, _ = e.kv.Set(v)
	e.events <- KVEvent{Type: KVEventPut, KVValue: v}
	return nil
}

func (e *ConfigManager) del(ctx context.Context, ns, key string) error {
	v, ok := e.kv.Delete(KVValue{Key: path.Clean(path.Join(ns, key))})
	if ok {
		e.events <- KVEvent{Type: KVEventPut, KVValue: v}
	}
	return nil
}

func (e *ConfigManager) Close() error {
	e.cancel()
	e.wg.Wait()
	return nil
}
