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
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/tidwall/btree"
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

type ConfigManager struct {
	wg     waitgroup.WaitGroup
	cancel context.CancelFunc
	logger *zap.Logger

	kv *btree.BTreeG[KVValue]

	wch     *fsnotify.Watcher
	overlay string
	sts     struct {
		sync.Mutex
		listeners []chan<- *config.Config
		current   *config.Config
	}
}

func NewConfigManager() *ConfigManager {
	return &ConfigManager{}
}

func (e *ConfigManager) Init(ctx context.Context, logger *zap.Logger, configFile string, overlay *config.Config) error {
	e.logger = logger

	// for namespace persistence
	e.kv = btree.NewBTreeG(func(a, b KVValue) bool {
		return a.Key < b.Key
	})

	// for config watch
	wch, err := fsnotify.NewWatcher()
	if err != nil {
		return errors.WithStack(err)
	}
	e.wch = wch

	var nctx context.Context
	nctx, e.cancel = context.WithCancel(ctx)
	e.wg.Run(func() {
		for {
			select {
			case <-nctx.Done():
				return
			case err := <-e.wch.Errors:
				e.logger.Info("failed to watch config file", zap.Error(err))
			case ev := <-e.wch.Events:
				e.handleFSEvent(ev)
			}
		}
	})

	if overlay != nil {
		ob, err := overlay.ToBytes()
		if err != nil {
			return errors.WithStack(err)
		}
		e.overlay = string(ob)
	}

	if configFile != "" {
		if err := e.reloadConfigFile(configFile); err != nil {
			return errors.WithStack(err)
		}
		if err := e.wch.Add(configFile); err != nil {
			return errors.WithStack(err)
		}
	} else {
		if err := e.SetTOMLConfig(nil); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (e *ConfigManager) Close() error {
	e.cancel()
	wcherr := e.wch.Close()
	e.wg.Wait()
	return wcherr
}
