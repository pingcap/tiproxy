// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"context"
	"fmt"
	"os"
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

var ErrNoResults = errors.Errorf("has no results")

type KVValue struct {
	Key   string
	Value []byte
}

type unknownConfigKeysWarning struct {
	keys []string
}

type ConfigManager struct {
	wg            waitgroup.WaitGroup
	cancel        context.CancelFunc
	advertiseAddr string

	kv *btree.BTreeG[KVValue]

	checkFileInterval time.Duration
	fileContent       []byte // used to compare whether the config file has changed
	fileLoaded        bool   // distinguishes an unread file from an empty file
	log               struct {
		sync.Mutex
		logger  *zap.Logger
		pending []unknownConfigKeysWarning
	}
	sts struct {
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

// SetLogger configures the logger and flushes warnings collected while loading
// the initial configuration, before the main logger was available.
func (e *ConfigManager) SetLogger(logger *zap.Logger) {
	if logger == nil {
		return
	}

	e.log.Lock()
	e.log.logger = logger
	pending := e.log.pending
	e.log.pending = nil
	e.log.Unlock()

	for _, warning := range pending {
		logUnknownConfigKeys(logger, warning)
	}
}

func (e *ConfigManager) warnUnknownConfigKeys(warning unknownConfigKeysWarning) {
	if len(warning.keys) == 0 {
		return
	}

	e.log.Lock()
	logger := e.log.logger
	if logger == nil {
		e.log.pending = append(e.log.pending, warning)
		e.log.Unlock()
		return
	}
	e.log.Unlock()

	logUnknownConfigKeys(logger, warning)
}

func logUnknownConfigKeys(logger *zap.Logger, warning unknownConfigKeysWarning) {
	logger.Warn("configuration contains unknown keys",
		zap.Strings("unknown_keys", warning.keys),
		zap.Int("unknown_key_count", len(warning.keys)),
	)
}

func (e *ConfigManager) Init(ctx context.Context, configFile string, advertiseAddr string) error {
	var nctx context.Context
	nctx, e.cancel = context.WithCancel(ctx)

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
						fmt.Fprintf(os.Stderr, "failed to reload file %s: %v", configFile, err)
					}
				}
			}
		}, nil, nil)
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
