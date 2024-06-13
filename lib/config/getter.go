// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

type ConfigGetter interface {
	GetConfig() *Config
}
