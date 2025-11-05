// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

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
	require.Empty(t, pkt.GetResp())
	// test write with save resp
	pkt.saveResp = true
	require.NoError(t, pkt.WritePacket([]byte("hello"), true))
	require.Equal(t, "hello", string(pkt.GetResp()))
	pkt.Reset()
	require.Equal(t, "", string(pkt.GetResp()))
	pkt.saveResp = true
	require.NoError(t, pkt.WritePacket([]byte("world"), false))
	require.Equal(t, "world", string(pkt.GetResp()))
	// test flush
	require.NoError(t, pkt.Flush())
}
