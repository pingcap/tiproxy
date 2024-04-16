// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import "sort"

const (
	// The threshold of ratio of the most connection count and least count.
	// If the ratio exceeds the threshold, we migrate connections.
	connBalancedRatio = 1.2
)

type FactorConnCount struct {
}

func NewFactorConnCount() *FactorConnCount {
	return &FactorConnCount{}
}

func (fcc *FactorConnCount) Name() string {
	return "factor_conn"
}

func (fcc *FactorConnCount) Route(backends []*backendWrapper) []*backendWrapper {
	if len(backends) == 0 {
		return backends
	}
	sort.Slice(backends, func(i, j int) bool {
		return backends[i].connList.Len() < backends[j].connList.Len()
	})
	connCount := backends[0].connList.Len()
	for i := range backends {
		if backends[i].connList.Len() > connCount {
			return backends[:i]
		}
	}
	return backends
}

func (fcc *FactorConnCount) Balance(backends []*backendWrapper) (fromBackends, toBackends []*backendWrapper) {
	if len(backends) == 0 {
		return backends, backends
	}
	sort.Slice(backends, func(i, j int) bool {
		return backends[i].connList.Len() < backends[j].connList.Len()
	})
	minConnCount, maxConnCount := backends[0].connList.Len(), backends[len(backends)-1].connList.Len()
	if float64(maxConnCount) <= float64(minConnCount+1)*connBalancedRatio {
		return nil, backends
	}
	for i, backend := range backends {
		if backend.connList.Len() > minConnCount {
			toBackends = backends[:i]
			break
		}
	}
	for i := len(backends) - 1; i >= 0; i-- {
		if backends[i].connList.Len() < maxConnCount {
			fromBackends = backends[i+1:]
			break
		}
	}
	return
}
