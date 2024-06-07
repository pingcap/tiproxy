// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import "github.com/pingcap/tiproxy/lib/config"

const (
	// locationLabelName indicates the label name that location-based balance should be based on.
	// We use `zone` because the follower read in TiDB also uses `zone` to decide location.
	locationLabelName = "zone"
	// balanceCount4Location indicates how many connections to balance per second.
	balanceCount4Location = 1
)

var _ Factor = (*FactorLabel)(nil)

type FactorLocation struct {
	// The location of this tiproxy instance.
	selfLocation string
	bitNum       int
}

func NewFactorLocation() *FactorLocation {
	return &FactorLocation{
		bitNum: 1,
	}
}

func (fl *FactorLocation) Name() string {
	return "location"
}

func (fl *FactorLocation) UpdateScore(backends []scoredBackend) {
	if len(fl.selfLocation) == 0 || len(backends) <= 1 {
		return
	}
	for i := 0; i < len(backends); i++ {
		score := 1
		backendLabels := backends[i].GetBackendInfo().Labels
		if backendLabels != nil && backendLabels[locationLabelName] == fl.selfLocation {
			score = 0
		}
		backends[i].addScore(score, fl.bitNum)
	}
}

func (fl *FactorLocation) ScoreBitNum() int {
	return fl.bitNum
}

func (fl *FactorLocation) BalanceCount(from, to scoredBackend) int {
	return balanceCount4Location
}

func (fl *FactorLocation) SetConfig(cfg *config.Config) {
	if cfg.Labels != nil {
		fl.selfLocation = cfg.Labels[locationLabelName]
	}
}
