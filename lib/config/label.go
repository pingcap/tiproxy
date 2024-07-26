// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

const (
	// LocationLabelName indicates the label name that decides the location of TiProxy and backends.
	// We use `zone` because the follower read in TiDB also uses `zone` to decide location.
	LocationLabelName = "zone"
)

func (cfg *Config) GetLocation() string {
	if len(cfg.Labels) == 0 {
		return ""
	}
	return cfg.Labels[LocationLabelName]
}
