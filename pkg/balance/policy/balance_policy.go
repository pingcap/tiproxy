// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package policy

import (
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"go.uber.org/zap"
)

type BalancePolicy interface {
	Init(cfg *config.Config)
	BackendToRoute(backends []BackendCtx) BackendCtx
	// balanceCount is the count of connections to balance per second.
	BackendsToBalance(backends []BackendCtx) (from, to BackendCtx, balanceCount int, reason []zap.Field)
	SetConfig(cfg *config.Config)
}

type BackendCtx interface {
	Addr() string
	// ConnCount indicates the count of current connections.
	ConnCount() int
	// ConnScore = current connections + incoming connections - outgoing connections.
	ConnScore() int
	Healthy() bool
	GetBackendInfo() observer.BackendInfo
}
