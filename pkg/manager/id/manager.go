// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package id

import "sync/atomic"

// IDManager is used to generate unique ID concurrently.
// SQLServer and Replay allocate backend connection ID conrrently, but the ID cannot overlap.
type IDManager struct {
	id atomic.Uint64
}

func NewIDManager() *IDManager {
	return &IDManager{}
}

func (m *IDManager) NewID() uint64 {
	return m.id.Add(1)
}
