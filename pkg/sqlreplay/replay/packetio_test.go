// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPacketIO(t *testing.T) {
	cli2SrvCh, srv2CliCh := make(chan []byte, 1), make(chan []byte, 1)
	pkt := newPacketIO(cli2SrvCh, srv2CliCh)
	defer pkt.Close()
	// test read
	cli2SrvCh <- []byte("hello")
	data, err := pkt.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), data)
	// test write
	require.NoError(t, pkt.WritePacket([]byte("wor"), false))
	require.NoError(t, pkt.WritePacket([]byte("ld"), true))
	data = <-srv2CliCh
	require.Equal(t, []byte("world"), data)
	// test flush
	require.NoError(t, pkt.WritePacket([]byte("hi"), false))
	require.NoError(t, pkt.Flush())
	data = <-srv2CliCh
	require.Equal(t, []byte("hi"), data)
}
