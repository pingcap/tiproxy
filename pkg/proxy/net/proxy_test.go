// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"bytes"
	"io"
	"net"
	"testing"

	"github.com/pingcap/tiproxy/pkg/proxy/proxyprotocol"
	"github.com/pingcap/tiproxy/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestProxyParse(t *testing.T) {
	tcpaddr, p := mockProxy(t)
	testPipeConn(t,
		func(t *testing.T, cli *packetIO) {
			b, err := p.ToBytes()
			require.NoError(t, err)
			_, err = io.Copy(cli.readWriter, bytes.NewReader(b))
			require.NoError(t, err)
			err = cli.WritePacket([]byte("hello"), true)
			require.NoError(t, err)
		},
		func(t *testing.T, srv *packetIO) {
			srv.ApplyOpts(WithProxy)
			b, err := srv.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, "hello", string(b))
			require.Equal(t, tcpaddr.String(), srv.RemoteAddr().String())
		},
		1,
	)
}

func TestProxyReadWrite(t *testing.T) {
	addr, p := mockProxy(t)
	message := []byte("hello world")
	testkit.TestTCPConn(t,
		func(t *testing.T, c net.Conn) {
			prw := newProxyClient(newBasicReadWriter(c, DefaultConnBufferSize), p)
			n, err := prw.Write(message)
			require.NoError(t, err)
			require.Equal(t, len(message), n)
			require.NoError(t, prw.Flush())
		},
		func(t *testing.T, c net.Conn) {
			prw := newProxyServer(newBasicReadWriter(c, DefaultConnBufferSize))
			data := make([]byte, len(message))
			n, err := prw.Read(data)
			require.NoError(t, err)
			require.Equal(t, len(message), n)

			parsedAddr, ok := prw.Proxy().SrcAddress.(*net.TCPAddr)
			require.True(t, ok)
			require.Equal(t, addr.IP.To4(), parsedAddr.IP)
			require.Equal(t, addr.Port, parsedAddr.Port)
			require.Equal(t, addr.String(), prw.RemoteAddr().String())
		}, 1)
}

func mockProxy(t *testing.T) (*net.TCPAddr, *proxyprotocol.Proxy) {
	tcpaddr, err := net.ResolveTCPAddr("tcp", "192.168.1.1:34")
	require.NoError(t, err)
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
	return tcpaddr, p
}
