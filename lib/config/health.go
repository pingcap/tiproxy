// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

type HealthInfo struct {
	ConfigChecksum uint32 `json:"config_checksum"`
}
