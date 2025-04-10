// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import "github.com/pingcap/tiproxy/pkg/balance/policy"

type scoredBackend struct {
	policy.BackendCtx
	// The score composed by all factors. Each factor sets some bits of the score.
	// The higher the score is, the more unhealthy / busy the backend is.
	scoreBits uint64
}

func newScoredBackend(backend policy.BackendCtx) scoredBackend {
	return scoredBackend{
		BackendCtx: backend,
	}
}

// prepareScore shifts the score bits before addScore.
func (b *scoredBackend) prepareScore(bitNum int) {
	b.scoreBits = b.scoreBits << bitNum
}

// addScore must be called after prepareScore.
func (b *scoredBackend) addScore(score int, bitNum int) {
	if score >= 1<<bitNum {
		score = 1<<bitNum - 1
	}
	b.scoreBits += uint64(score)
}

// score returns the total score.
func (b *scoredBackend) score() uint64 {
	return b.scoreBits
}

// factorScore gets the score for a factor.
func (b *scoredBackend) factorScore(bitNum int) int {
	return int(b.scoreBits & ((1 << bitNum) - 1))
}
