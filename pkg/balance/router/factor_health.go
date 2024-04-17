// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import "sort"

const (
	// balanceCount4Health indicates how many connections to balance per second.
	// If some backends are unhealthy, migrate fast but do not put too much pressure on TiDB.
	balanceCount4Health = 1000
)

var _ Factor = (*FactorHealth)(nil)

type FactorHealth struct {
}

func NewFactorHealth() *FactorHealth {
	return &FactorHealth{}
}

func (fh *FactorHealth) Name() string {
	return "health"
}

func (fh *FactorHealth) Route(backends []*backendWrapper) []*backendWrapper {
	if len(backends) == 0 {
		return backends
	}
	sort.Slice(backends, func(i, j int) bool {
		return backends[i].Healthy() && !backends[j].Healthy()
	})
	for i, backend := range backends {
		if !backend.Healthy() {
			return backends[:i]
		}
	}
	return backends
}

func (fh *FactorHealth) Balance(backends []*backendWrapper) BalanceHint {
	if len(backends) == 0 {
		return BalanceHint{
			tp: typeNoBackends,
		}
	}
	// healthy < unhealthy without connections < unhealthy with connections
	sort.Slice(backends, func(i, j int) bool {
		if !backends[j].Healthy() {
			if backends[i].Healthy() {
				return true
			}
			return backends[i].ConnCount() < backends[j].ConnCount()
		}
		return false
	})

	healthyLast := -1
	for i, backend := range backends {
		if !backend.Healthy() {
			break
		}
		healthyLast = i
	}
	// All the backends are unhealthy.
	if healthyLast < 0 {
		return BalanceHint{
			tp: typeNoBackends,
		}
	}

	unhealthyFirst := len(backends)
	for i := len(backends) - 1; i >= 0; i-- {
		if backends[i].Healthy() || backends[i].ConnCount() == 0 {
			break
		}
		unhealthyFirst = i
	}
	// All the backends are healthy, or no unhealthy backends have connections.
	if unhealthyFirst >= len(backends) {
		return BalanceHint{
			tp:         typeBalanced,
			toBackends: backends[:healthyLast+1],
		}
	}

	// Unbalanced.
	return BalanceHint{
		tp:           typeUnbalanced,
		fromBackends: backends[unhealthyFirst:],
		toBackends:   backends[:healthyLast+1],
		connCount:    balanceCount4Health,
	}
}
