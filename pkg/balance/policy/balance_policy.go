// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package policy

import (
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"go.uber.org/zap"
)

type BalancePolicy interface {
	Init()
	BackendToRoute(backends []BackendCtx) BackendCtx
	BackendsToBalance(backends []BackendCtx) (from, to BackendCtx, balanceCount int, reason []zap.Field)
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
