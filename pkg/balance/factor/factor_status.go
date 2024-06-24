// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import "github.com/pingcap/tiproxy/lib/config"

const (
	// balanceSeconds4Status indicates the time (in seconds) to migrate all the connections.
	// A typical TiDB graceful shutdown time is 15s. The health check delay is 3s and the max transaction duration may be seconds.
	// We need to migrate connections in time but it can't be too fast because the table statistics of the target backend
	// may be loaded slowly.
	balanceSeconds4Status = 5.0
)

// The snapshot of backend data of the last round.
type statusBackendSnapshot struct {
	// Record the balance count when the backend becomes unhealthy so that it won't be smaller in the next rounds.
	balanceCount float64
}

var _ Factor = (*FactorStatus)(nil)

type FactorStatus struct {
	snapshot map[string]statusBackendSnapshot
	bitNum   int
}

func NewFactorStatus() *FactorStatus {
	return &FactorStatus{
		bitNum:   1,
		snapshot: make(map[string]statusBackendSnapshot),
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
	snapshots := make(map[string]statusBackendSnapshot, len(backends))
	for i := 0; i < len(backends); i++ {
		var balanceCount float64
		addr := backends[i].Addr()
		if !backends[i].Healthy() {
			snapshot, existSnapshot := fs.snapshot[addr]
			if existSnapshot && snapshot.balanceCount > 0.0001 {
				balanceCount = snapshot.balanceCount
			} else {
				balanceCount = float64(backends[i].ConnScore()) / balanceSeconds4Status
			}
		}
		snapshots[addr] = statusBackendSnapshot{
			balanceCount: balanceCount,
		}
	}
	fs.snapshot = snapshots
}

func (fs *FactorStatus) ScoreBitNum() int {
	return fs.bitNum
}

func (fs *FactorStatus) BalanceCount(from, to scoredBackend) float64 {
	return fs.snapshot[from.Addr()].balanceCount
}

func (fs *FactorStatus) SetConfig(cfg *config.Config) {
}

func (fs *FactorStatus) Close() {
}
