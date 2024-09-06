// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPacketIO(t *testing.T) {
	pkt := newPacketIO()
	defer pkt.Close()
	// test read
	_, err := pkt.ReadPacket()
	require.Error(t, err)
	// test write
	require.NoError(t, pkt.WritePacket([]byte("hello"), true))
	require.NoError(t, pkt.WritePacket([]byte("world"), false))
	// test flush
	require.NoError(t, pkt.Flush())
}
