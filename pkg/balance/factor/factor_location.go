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

var _ Factor = (*FactorLocation)(nil)

type FactorLocation struct {
	bitNum              int
	migrationsPerSecond float64
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

func (fl *FactorLocation) BalanceCount(from, to scoredBackend) (BalanceAdvice, float64, []zap.Field) {
	count := float64(balanceCount4Location)
	if fl.migrationsPerSecond > 0 {
		count = fl.migrationsPerSecond
	}
	return AdvicePositive, count, nil
}

func (fl *FactorLocation) SetConfig(cfg *config.Config) {
	fl.migrationsPerSecond = cfg.Balance.Location.MigrationsPerSecond
}

func (fl *FactorLocation) CanBeRouted(_ uint64) bool {
	return true
}

func (fl *FactorLocation) Close() {
}
