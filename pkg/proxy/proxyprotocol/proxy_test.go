// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package proxyprotocol

import (
	"bytes"
	"io"
	"net"
	"testing"

	"github.com/pingcap/TiProxy/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestProxyParse(t *testing.T) {
	tcpaddr, err := net.ResolveTCPAddr("tcp", "192.168.1.1:34")
	require.NoError(t, err)

	testkit.TestPipeConn(t,
		func(t *testing.T, cli net.Conn) {
			p := &Proxy{
				Version:    ProxyVersion2,
				Command:    ProxyCommandLocal,
				SrcAddress: tcpaddr,
				DstAddress: tcpaddr,
				TLV: []ProxyTlv{
					{
						Typ:     ProxyTlvALPN,
						Content: nil,
					},
					{
						Typ:     ProxyTlvUniqueID,
						Content: []byte("test"),
					},
				},
			}
			b, err := p.ToBytes()
			require.NoError(t, err)
			_, err = io.Copy(cli, bytes.NewReader(b))
			require.NoError(t, err)
		},
		func(t *testing.T, srv net.Conn) {
			// skip magic
			hdr := make([]byte, len(MagicV2))
			_, err := io.ReadFull(srv, hdr)
			require.NoError(t, err)

			// try to parse V2
			p, _, err := ParseProxyV2(srv)
			require.NoError(t, err)
			require.NotNil(t, p)
			require.Equal(t, tcpaddr, p.SrcAddress)
			require.Equal(t, tcpaddr, p.DstAddress)
			require.Equal(t, ProxyVersion2, p.Version)
			require.Equal(t, ProxyCommandLocal, p.Command)
			require.Len(t, p.TLV, 2)
			require.Equal(t, ProxyTlvALPN, p.TLV[0].Typ)
			require.Equal(t, ProxyTlvUniqueID, p.TLV[1].Typ)
			require.Equal(t, []byte("test"), p.TLV[1].Content)
		},
		1,
	)
}

type originAddr struct {
	net.Addr
}

func (a *originAddr) Unwrap() net.Addr {
	return a.Addr
}

func TestProxyToBytes(t *testing.T) {
	hdr := &Proxy{
		Version:    ProxyVersion2,
		Command:    ProxyCommandLocal,
		SrcAddress: &net.TCPAddr{IP: make(net.IP, net.IPv4len), Port: 0},
		DstAddress: &net.TCPAddr{IP: make(net.IP, net.IPv6len), Port: 0},
	}
	hdrBytes, err := hdr.ToBytes()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(hdrBytes), len(MagicV2)+4)
	length := int(hdrBytes[len(MagicV2)+2])<<8 | int(hdrBytes[len(MagicV2)+3])
	require.Equal(t, len(hdrBytes)-4-len(MagicV2), length)

	hdr.DstAddress = &net.UDPAddr{}
	_, err = hdr.ToBytes()
	require.ErrorIs(t, err, ErrAddressFamilyMismatch)

	hdr.DstAddress = &originAddr{Addr: &net.TCPAddr{IP: make(net.IP, net.IPv6len), Port: 0}}
	_, err = hdr.ToBytes()
	require.NoError(t, err)
}
