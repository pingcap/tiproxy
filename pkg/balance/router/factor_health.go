// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import "sort"

type FactorHealth struct {
}

func NewFactorHealth() *FactorHealth {
	return &FactorHealth{}
}

func (fh *FactorHealth) Name() string {
	return "factor_health"
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

func (fh *FactorHealth) Balance(backends []*backendWrapper) (fromBackends, toBackends []*backendWrapper) {
	if len(backends) == 0 {
		return backends, backends
	}
	sort.Slice(backends, func(i, j int) bool {
		return backends[i].Healthy() && !backends[j].Healthy()
	})
	for i, backend := range backends {
		if !backend.Healthy() {
			return backends[:i], backends[i+1:]
		}
	}
	return nil, backends
}
