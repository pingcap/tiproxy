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
	"fmt"
	"path"
	"time"

	"github.com/pingcap/TiProxy/pkg/config"
	"github.com/pingcap/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	DefaultEtcdDialTimeout = 3 * time.Second
	DefaultEtcdPath        = "/config"

	PathPrefixNamespace = "ns"
)

type ConfigManager struct {
	logger     *zap.Logger
	etcdClient *clientv3.Client
	kv         clientv3.KV
	basePath   string
	cfg        config.ConfigManager
}

func NewConfigManager() *ConfigManager {
	return &ConfigManager{}
}

func (srv *ConfigManager) Init(addrs []string, cfg config.ConfigManager, logger *zap.Logger) error {
	srv.logger = logger
	srv.cfg = cfg
	// slash appended to distinguish '/dir'(file) and '/dir/'(directory)
	srv.basePath = appendSlashToDirPath(DefaultEtcdPath)

	etcdConfig := clientv3.Config{
		Endpoints:   addrs,
		DialTimeout: DefaultEtcdDialTimeout,
	}

	etcdClient, err := clientv3.New(etcdConfig)
	if err != nil {
		return errors.WithMessage(err, "create etcd config center error")
	}
	srv.etcdClient = etcdClient
	srv.kv = clientv3.NewKV(srv.etcdClient)

	return nil
}

func (e *ConfigManager) get(ctx context.Context, ns, key string) (*mvccpb.KeyValue, error) {
	resp, err := e.getMul(ctx, ns, key)
	if err != nil {
		return nil, err
	}
	if len(resp) != 1 {
		return nil, fmt.Errorf("has no results or multiple results")
	}
	return resp[0], nil
}

func (e *ConfigManager) getMul(ctx context.Context, ns, key string) ([]*mvccpb.KeyValue, error) {
	resp, err := e.kv.Get(ctx, path.Join(e.basePath, ns, key))
	if err != nil {
		return nil, err
	}
	return resp.Kvs, nil
}

func (e *ConfigManager) list(ctx context.Context, ns string) ([]*mvccpb.KeyValue, error) {
	resp, err := e.kv.Get(ctx, path.Join(e.basePath, ns), clientv3.WithPrefix())
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

func (e *ConfigManager) del(ctx context.Context, ns, key string) ([]*mvccpb.KeyValue, error) {
	resp, err := e.kv.Delete(ctx, path.Join(e.basePath, ns, key))
	if err != nil {
		return nil, err
	}
	return resp.PrevKvs, nil
}

func (e *ConfigManager) Close() error {
	if err := e.etcdClient.Close(); err != nil {
		e.logger.Error("fail to close config manager", zap.Error(err))
		return err
	}
	return nil
}

func appendSlashToDirPath(dir string) string {
	if len(dir) == 0 || dir[len(dir)-1] == '/' {
		return dir
	}
	return dir + "/"
}
