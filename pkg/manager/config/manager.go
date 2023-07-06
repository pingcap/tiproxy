// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

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
		checksum  uint32
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
					e.logger.Warn("failed to watch config file", zap.Error(err))
				case ev := <-e.wch.Events:
					e.handleFSEvent(ev, configFile)
				case <-ticker.C:
					// There may be concurrent issues:
					// 1. Remove the directory and the watcher removes the directory automatically
					// 2. Create the directory and the file again within a tick
					// 3. Add it to the watcher again, but the CREATE event is not sent
					//
					// Checking the watch list still have a concurrent issue because the watcher may remove the
					// directory between WatchList and Add. We'll fix it later because it's complex to fix it entirely.
					exists := len(e.wch.WatchList()) > 0
					if err := e.wch.Add(parentDir); err != nil {
						e.logger.Warn("failed to rewatch config file", zap.Error(err))
					} else if !exists {
						e.logger.Info("config file reloaded", zap.Error(e.reloadConfigFile(configFile)))
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
	if e.cancel != nil {
		e.cancel()
		e.cancel = nil
	}
	if e.wch != nil {
		wcherr = e.wch.Close()
		e.wch = nil
	}
	e.sts.Lock()
	for _, ch := range e.sts.listeners {
		close(ch)
	}
	e.sts.listeners = nil
	e.sts.Unlock()
	e.wg.Wait()
	return wcherr
}
