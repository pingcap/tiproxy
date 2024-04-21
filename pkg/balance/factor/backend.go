// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

type Backend interface {
	// ConnScore = current connections + incoming connections - outgoing connections.
	ConnScore() int
	Healthy() bool
}

type scoredBackend struct {
	Backend
	// The score composed by all factors. Each factor sets some bits of the score.
	// The higher the score is, the more unhealthy / busy the backend is.
	scoreBits uint64
}

func newScoredBackend(backend Backend) scoredBackend {
	return scoredBackend{
		Backend: backend,
	}
}

func (b *scoredBackend) addScore(score int, bitNum int) {
	if score >= 1<<bitNum {
		score = 1<<bitNum - 1
	}
	b.scoreBits = b.scoreBits<<bitNum | uint64(score)
}

func (b *scoredBackend) score() uint64 {
	return b.scoreBits
}
