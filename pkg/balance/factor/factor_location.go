// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"github.com/pingcap/tiproxy/lib/config"
	"go.uber.org/zap"
)

const (
	// balanceCount4Location indicates how many connections to balance per second.
	balanceCount4Location = 1
)

var _ Factor = (*FactorLabel)(nil)

type FactorLocation struct {
	bitNum int
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
	if len(backends) <= 1 {
		return
	}
	for i := 0; i < len(backends); i++ {
		score := 1
		if backends[i].Local() {
			score = 0
		}
		backends[i].addScore(score, fl.bitNum)
	}
}

func (fl *FactorLocation) ScoreBitNum() int {
	return fl.bitNum
}

func (fl *FactorLocation) BalanceCount(from, to scoredBackend) (float64, []zap.Field) {
	return balanceCount4Location, nil
}

func (fl *FactorLocation) SetConfig(cfg *config.Config) {
}

func (fl *FactorLocation) Close() {
}
