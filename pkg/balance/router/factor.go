// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

type Factor interface {
	// Name returns the name of the factor.
	Name() string
	// UpdateScore updates the score of each backend.
	UpdateScore(backends []*backendWrapper)
	// ScoreBitNum returns the bit number of the score.
	ScoreBitNum() int
	// BalanceCount returns the count of connections to balance in this round.
	// 0 indicates balanced (within the threshold) or the migration speed is limited.
	BalanceCount(from, to *backendWrapper) int
}
