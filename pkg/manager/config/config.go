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
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/fsnotify/fsnotify"
	"github.com/pingcap/TiProxy/lib/config"
	"go.uber.org/zap"
)

func (e *ConfigManager) reloadConfigFile(file string) error {
	proxyConfigData, err := os.ReadFile(file)
	if err != nil {
		return err
	}

	return e.SetTOMLConfig(proxyConfigData)
}

func (e *ConfigManager) handleFSEvent(ev fsnotify.Event, f string) {
	switch {
	case ev.Has(fsnotify.Create), ev.Has(fsnotify.Write), ev.Has(fsnotify.Remove), ev.Has(fsnotify.Rename):
		if ev.Has(fsnotify.Remove) || ev.Has(fsnotify.Rename) {
			// in case of remove/rename the file, files are not present at filesystem for a while
			// it may be too fast to read the config file now, sleep for a while
			time.Sleep(50 * time.Millisecond)
		}
		// try to reload it
		e.logger.Info("config file reloaded", zap.Error(e.reloadConfigFile(f)), zap.Any("cfg", e.GetConfig()))
	}
}

// SetTOMLConfig will do partial config update. Usually, user will expect config changes
// only when they specified a config item. It is, however, impossible to tell a struct
// `c.max-conns == 0` means no user-input, or it specified `0`.
// So we always update the current config with a TOML string, which only overwrite fields
// that are specified by users.
func (e *ConfigManager) SetTOMLConfig(data []byte) error {
	e.sts.Lock()
	defer e.sts.Unlock()

	base := e.sts.current
	if base == nil {
		base = config.NewConfig()
	} else {
		base = base.Clone()
	}

	if err := toml.Unmarshal(data, base); err != nil {
		return err
	}

	if err := toml.Unmarshal(e.overlay, base); err != nil {
		return err
	}

	if err := base.Check(); err != nil {
		return err
	}

	e.sts.current = base
	e.sts.version++

	for _, list := range e.sts.listeners {
		list <- base.Clone()
	}

	return nil
}

func (e *ConfigManager) GetConfig() *config.Config {
	e.sts.Lock()
	v := e.sts.current
	e.sts.Unlock()
	return v
}

func (e *ConfigManager) GetConfigVersion() uint32 {
	e.sts.Lock()
	v := e.sts.version
	e.sts.Unlock()
	return v
}

func (e *ConfigManager) WatchConfig() <-chan *config.Config {
	ch := make(chan *config.Config)
	e.sts.Lock()
	e.sts.listeners = append(e.sts.listeners, ch)
	e.sts.Unlock()
	return ch
}
