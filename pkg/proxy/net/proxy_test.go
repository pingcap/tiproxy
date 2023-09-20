// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"bytes"
	"io"
	"net"
	"testing"

	"github.com/pingcap/tiproxy/pkg/proxy/proxyprotocol"
	"github.com/stretchr/testify/require"
)

func TestProxyParse(t *testing.T) {
	tcpaddr, err := net.ResolveTCPAddr("tcp", "192.168.1.1:34")
	require.NoError(t, err)

	testPipeConn(t,
		func(t *testing.T, cli *PacketIO) {
			p := &proxyprotocol.Proxy{
				Version:    proxyprotocol.ProxyVersion2,
				Command:    proxyprotocol.ProxyCommandLocal,
				SrcAddress: tcpaddr,
				DstAddress: tcpaddr,
				TLV: []proxyprotocol.ProxyTlv{
					{
						Typ:     proxyprotocol.ProxyTlvALPN,
						Content: nil,
					},
					{
						Typ:     proxyprotocol.ProxyTlvUniqueID,
						Content: []byte("test"),
					},
				},
			}
			b, err := p.ToBytes()
			require.NoError(t, err)
			_, err = io.Copy(cli.readWriter, bytes.NewReader(b))
			require.NoError(t, err)
			err = cli.WritePacket([]byte("hello"), true)
			require.NoError(t, err)
		},
		func(t *testing.T, srv *PacketIO) {
			srv.ApplyOpts(WithProxy)
			b, err := srv.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, "hello", string(b))
			require.Equal(t, tcpaddr.String(), srv.RemoteAddr().String())
		},
		1,
	)
}
