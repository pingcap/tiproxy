// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"go.uber.org/zap"
)

const (
	// balanceSeconds4Status indicates the time (in seconds) to migrate all the connections.
	// A typical TiDB graceful shutdown time is 15s. The health check delay is 3s and the max transaction duration may be seconds.
	// We need to migrate connections in time but it can't be too fast because the table statistics of the target backend
	// may be loaded slowly.
	balanceSeconds4Status = 5.0
	backendExpiration     = time.Minute
)

// The snapshot of backend data of the last round.
type statusBackendSnapshot struct {
	// The last time that the backend calculates the score. If the backend is not in the list for a long time, prune it.
	lastAccess time.Time
	// Record the balance count when the backend becomes unhealthy so that it won't be smaller in the next rounds.
	balanceCount float64
}

var _ Factor = (*FactorStatus)(nil)

type FactorStatus struct {
	snapshot            map[string]statusBackendSnapshot
	bitNum              int
	migrationsPerSecond float64
	lg                  *zap.Logger
}

func NewFactorStatus(lg *zap.Logger) *FactorStatus {
	return &FactorStatus{
		bitNum:   1,
		snapshot: make(map[string]statusBackendSnapshot),
		lg:       lg,
	}
}

func (fs *FactorStatus) Name() string {
	return "status"
}

func (fs *FactorStatus) UpdateScore(backends []scoredBackend) {
	fs.updateSnapshot(backends)
	for i := 0; i < len(backends); i++ {
		score := 0
		if !backends[i].Healthy() {
			score = 1
		}
		backends[i].addScore(score, fs.bitNum)
	}
}

func (fs *FactorStatus) updateSnapshot(backends []scoredBackend) {
	now := time.Now()
	for i := 0; i < len(backends); i++ {
		addr := backends[i].Addr()
		if backends[i].Healthy() {
			delete(fs.snapshot, addr)
			continue
		}
		snapshot := fs.snapshot[addr]
		// The rebalance was already started, don't update it.
		if snapshot.balanceCount > 0.0001 {
			snapshot.lastAccess = now
			fs.snapshot[addr] = snapshot
			continue
		}
		balanceCount := float64(backends[i].ConnScore()) / balanceSeconds4Status
		// Do not log it when the balance counts are both 0.
		if balanceCount != snapshot.balanceCount {
			fs.lg.Info("update status risk",
				zap.String("addr", addr),
				zap.Float64("balance_count", balanceCount),
				zap.Int("conn_score", backends[i].ConnScore()))
		}
		fs.snapshot[addr] = statusBackendSnapshot{
			balanceCount: balanceCount,
			lastAccess:   now,
		}
	}

	// An unhealthy backend may sometimes be not passed in, but its balanceCount should be preserved.
	// We only delete the backends that are not accessed for a while.
	for addr, backend := range fs.snapshot {
		if backend.lastAccess.Add(backendExpiration).Before(now) {
			delete(fs.snapshot, addr)
		}
	}
}

func (fs *FactorStatus) ScoreBitNum() int {
	return fs.bitNum
}

func (fs *FactorStatus) BalanceCount(from, to scoredBackend) (BalanceAdvice, float64, []zap.Field) {
	if fs.migrationsPerSecond > 0 {
		return AdvicePositive, fs.migrationsPerSecond, nil
	}
	return AdvicePositive, fs.snapshot[from.Addr()].balanceCount, nil
}

func (fs *FactorStatus) SetConfig(cfg *config.Config) {
	fs.migrationsPerSecond = cfg.Balance.Status.MigrationsPerSecond
}

func (fl *FactorStatus) CanBeRouted(score uint64) bool {
	return score == 0
}

func (fs *FactorStatus) Close() {
}
