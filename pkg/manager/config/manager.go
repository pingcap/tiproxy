// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/tidwall/btree"
	"go.uber.org/zap"
)

const (
	pathPrefixNamespace = "ns"
	pathPrefixConfig    = "config"
)

var (
	// Define it as a variable because tests will modify it.
	checkFileInterval = time.Second
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

	lastModTime time.Time
	overlay     []byte
	sts         struct {
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
		if err := e.checkFileAndLoad(configFile); err != nil {
			return errors.WithStack(err)
		}
		e.wg.Run(func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			for {
				select {
				case <-nctx.Done():
					return
				case <-ticker.C:
					if err = e.checkFileAndLoad(configFile); err != nil {
						e.logger.Debug("reload config file failed", zap.Error(err))
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

func (e *ConfigManager) checkFileAndLoad(filename string) error {
	info, err := os.Stat(filename)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return errors.New("config file is a directory")
	}
	if info.ModTime() != e.lastModTime {
		if err = e.reloadConfigFile(filename); err != nil {
			return err
		}
		e.logger.Info("config file reloaded", zap.Time("file_modify_time", info.ModTime()))
		e.lastModTime = info.ModTime()
	}
	return nil
}

func (e *ConfigManager) Close() error {
	if e.cancel != nil {
		e.cancel()
		e.cancel = nil
	}
	e.sts.Lock()
	for _, ch := range e.sts.listeners {
		close(ch)
	}
	e.sts.listeners = nil
	e.sts.Unlock()
	e.wg.Wait()
	return nil
}
