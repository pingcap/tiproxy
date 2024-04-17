// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import "sort"

const (
	// The threshold of ratio of the most connection count and least count.
	// If the ratio exceeds the threshold, we migrate connections.
	connBalancedRatio = 1.2
	// balanceSeconds4Conn indicates the time left to balance 2 backends.
	// Migrate the connections slow and steady, otherwise it may cause CPU unbalanced.
	// The actual time is much longer because it will migrate slower and slower.
	balanceSeconds4Conn = 60
)

var _ Factor = (*FactorConnCount)(nil)

type FactorConnCount struct {
}

func NewFactorConnCount() *FactorConnCount {
	return &FactorConnCount{}
}

func (fcc *FactorConnCount) Name() string {
	return "conn"
}

func (fcc *FactorConnCount) Route(backends []*backendWrapper) []*backendWrapper {
	if len(backends) == 0 {
		return backends
	}
	// Use connScore instead of connCount, otherwise all the connections are routed to the same one at once.
	sort.Slice(backends, func(i, j int) bool {
		return backends[i].connScore < backends[j].connScore
	})
	connScore := backends[0].connScore
	for i := range backends {
		if backends[i].connScore > connScore {
			return backends[:i]
		}
	}
	return backends
}

func (fcc *FactorConnCount) Balance(backends []*backendWrapper) BalanceHint {
	if len(backends) == 0 {
		return BalanceHint{
			tp: typeNoBackends,
		}
	}
	sort.Slice(backends, func(i, j int) bool {
		return backends[i].connScore < backends[j].connScore
	})
	minConnCount, maxConnCount := backends[0].connScore, backends[len(backends)-1].connScore
	gap := float64(maxConnCount) - float64(minConnCount+1)*connBalancedRatio
	if gap <= 0 {
		// Already balanced.
		return BalanceHint{
			tp:         typeBalanced,
			toBackends: backends,
		}
	}
	hint := BalanceHint{
		tp: typeUnbalanced,
	}
	for i, backend := range backends {
		if backend.connScore > minConnCount {
			hint.toBackends = backends[:i]
			break
		}
	}
	for i := len(backends) - 1; i >= 0; i-- {
		if backends[i].connScore < maxConnCount {
			hint.fromBackends = backends[i+1:]
			break
		}
	}
	// connCount is at least 1.
	hint.connCount = int(gap/(1+connBalancedRatio)/balanceSeconds4Conn + 1)
	return hint
}
