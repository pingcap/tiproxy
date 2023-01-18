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
	"path/filepath"
	"sync"
	"time"

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
	overlay []byte
	sts     struct {
		sync.Mutex
		listeners []chan<- *config.Config
		current   *config.Config
		version   uint32
	}
}

func NewConfigManager() *ConfigManager {
	return &ConfigManager{}
}

func (e *ConfigManager) Init(ctx context.Context, logger *zap.Logger, configFile string, overlay *config.Config) error {
	var err error
	var nctx context.Context
	nctx, e.cancel = context.WithCancel(ctx)

	e.logger = logger

	// for namespace persistence
	e.kv = btree.NewBTreeG(func(a, b KVValue) bool {
		return a.Key < b.Key
	})

	// for config watch
	if overlay != nil {
		e.overlay, err = overlay.ToBytes()
		if err != nil {
			return errors.WithStack(err)
		}
	}

	if configFile != "" {
		e.wch, err = fsnotify.NewWatcher()
		if err != nil {
			return errors.WithStack(err)
		}

		// Watch the parent dir, because vim/k8s or other apps may not edit files in-place:
		// e.g. k8s configmap is a symlink of a symlink to a file, which will only trigger
		// a remove event for the file.
		parentDir := filepath.Dir(configFile)

		if err := e.reloadConfigFile(configFile); err != nil {
			return errors.WithStack(err)
		}
		if err := e.wch.Add(parentDir); err != nil {
			return errors.WithStack(err)
		}

		e.wg.Run(func() {
			// Some apps will trigger rename/remove events, which means they will re-create/rename
			// the new file to the directory. Watch possibly stopped after rename/remove events.
			// So, we use a tick to repeatedly add the parent dir to re-watch files.
			ticker := time.NewTicker(200 * time.Millisecond)
			for {
				select {
				case <-nctx.Done():
					return
				case err := <-e.wch.Errors:
					e.logger.Info("failed to watch config file", zap.Error(err))
				case ev := <-e.wch.Events:
					e.handleFSEvent(ev, configFile)
				case <-ticker.C:
					if err := e.wch.Add(parentDir); err != nil {
						e.logger.Info("failed to rewatch config file", zap.Error(err))
					}
				}
			}
		})
	} else {
		if err := e.SetTOMLConfig(nil); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (e *ConfigManager) Close() error {
	var wcherr error
	e.cancel()
	if e.wch != nil {
		wcherr = e.wch.Close()
	}
	e.sts.Lock()
	for _, ch := range e.sts.listeners {
		close(ch)
	}
	e.sts.Unlock()
	e.wg.Wait()
	return wcherr
}
