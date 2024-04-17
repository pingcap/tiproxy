// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

type balanceType int

const (
	// The factor is already balanced, skip it.
	typeBalanced balanceType = iota
	// The factor is unbalanced, we need migration.
	typeUnbalanced
	// No available backends, we can do nothing.
	typeNoBackends
)

type BalanceHint struct {
	tp balanceType
	// The backends to be migrated from and to be migrated to.
	// NOTICE: DO NOT append to the returned slices in the caller!!!
	// Even if the factor is balanced, toBackends should be set to exclude the unhealthy backends (but without connections).
	fromBackends, toBackends []*backendWrapper
	// The count to balance in this round. If it's 0 but the factor is unbalanced, maybe it's limiting
	// the migration speed on purpose.
	connCount int
}

type Factor interface {
	// Name returns the name of the factor.
	Name() string
	// Route returns the backends that a new connection should be routed to.
	// If multiple backends are healthy and balanced, return them all.
	Route(backends []*backendWrapper) []*backendWrapper
	// Balance returns the source and target backends that the router should migrate connections from and to.
	Balance(backends []*backendWrapper) BalanceHint
}
