// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	"github.com/tidwall/btree"
	"go.uber.org/zap"
)

const (
	pathPrefixNamespace = "ns"
	pathPrefixConfig    = "config"
)

const (
	checkFileInterval = 2 * time.Second
)

var (
	ErrNoResults = errors.Errorf("has no results")
)

type KVValue struct {
	Key   string
	Value []byte
}

type ConfigManager struct {
	wg            waitgroup.WaitGroup
	cancel        context.CancelFunc
	logger        *zap.Logger
	advertiseAddr string

	kv *btree.BTreeG[KVValue]

	checkFileInterval time.Duration
	fileContent       []byte // used to compare whether the config file has changed
	sts               struct {
		sync.Mutex
		listeners []chan<- *config.Config
		current   *config.Config
		data      []byte // used to strictly compare whether the config has changed
		checksum  uint32 // checksum of the unmarshalled toml
	}
}

func NewConfigManager() *ConfigManager {
	return &ConfigManager{
		checkFileInterval: checkFileInterval,
	}
}

func (e *ConfigManager) Init(ctx context.Context, logger *zap.Logger, configFile string, advertiseAddr string) error {
	var nctx context.Context
	nctx, e.cancel = context.WithCancel(ctx)

	e.logger = logger
	e.advertiseAddr = advertiseAddr

	// for namespace persistence
	e.kv = btree.NewBTreeG(func(a, b KVValue) bool {
		return a.Key < b.Key
	})

	if configFile != "" {
		if err := e.reloadConfigFile(configFile); err != nil {
			return err
		}

		e.wg.RunWithRecover(func() {
			// Read the file periodically and reload the config if it changes.
			//
			// We tried other ways to watch file:
			// - Watch the directory by fsnotify, but it may not work well when the directory is removed and recreated immediately.
			// - Read the file modification time periodically, but it may not work well when the file is modified twice in 1 millisecond.
			ticker := time.NewTicker(e.checkFileInterval)
			for {
				select {
				case <-nctx.Done():
					ticker.Stop()
					return
				case <-ticker.C:
					if err := e.reloadConfigFile(configFile); err != nil {
						e.logger.Warn("failed to reload file", zap.String("file", configFile), zap.Error(err))
					}
				}
			}
		}, nil, e.logger)
	} else {
		if err := e.SetTOMLConfig(nil); err != nil {
			return err
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
	e.sts.Lock()
	for _, ch := range e.sts.listeners {
		close(ch)
	}
	e.sts.listeners = nil
	e.sts.Unlock()
	e.wg.Wait()
	return wcherr
}
