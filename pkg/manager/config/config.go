// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"
	"hash/crc32"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"go.uber.org/zap"
)

func (e *ConfigManager) reloadConfigFile(file string) error {
	proxyConfigData, err := os.ReadFile(file)
	if err != nil {
		return errors.WithStack(err)
	}

	return e.SetTOMLConfig(proxyConfigData)
}

// SetTOMLConfig will do partial config update. Usually, user will expect config changes
// only when they specified a config item. It is, however, impossible to tell a struct
// `c.max-conns == 0` means no user-input, or it specified `0`.
// So we always update the current config with a TOML string, which only overwrite fields
// that are specified by users.
func (e *ConfigManager) SetTOMLConfig(data []byte) (err error) {
	e.sts.Lock()
	defer func() {
		if err == nil {
			e.logger.Info("current config", zap.Any("cfg", e.sts.current))
		}
		e.sts.Unlock()
	}()

	base := e.sts.current
	if base == nil {
		base = config.NewConfig()
	} else {
		base = base.Clone()
	}

	if err = toml.Unmarshal(data, base); err != nil {
		return errors.WithStack(err)
	}

	if err = toml.Unmarshal(e.overlay, base); err != nil {
		return errors.WithStack(err)
	}

	if err = base.Check(); err != nil {
		return
	}

	e.sts.current = base
	var buf bytes.Buffer
	if err = toml.NewEncoder(&buf).Encode(base); err != nil {
		return errors.WithStack(err)
	}
	e.sts.checksum = crc32.ChecksumIEEE(buf.Bytes())

	for _, list := range e.sts.listeners {
		list <- base.Clone()
	}

	return
}

func (e *ConfigManager) GetConfig() *config.Config {
	e.sts.Lock()
	v := e.sts.current
	e.sts.Unlock()
	return v
}

func (e *ConfigManager) GetConfigChecksum() uint32 {
	e.sts.Lock()
	c := e.sts.checksum
	e.sts.Unlock()
	return c
}

func (e *ConfigManager) WatchConfig() <-chan *config.Config {
	ch := make(chan *config.Config)
	e.sts.Lock()
	e.sts.listeners = append(e.sts.listeners, ch)
	e.sts.Unlock()
	return ch
}
