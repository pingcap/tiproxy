// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package proxyprotocol

import (
	"bytes"
	"io"
	"net"
	"testing"

	"github.com/pingcap/tiproxy/pkg/testkit"
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

			srcAddr, ok := p.SrcAddress.(*net.TCPAddr)
			require.True(t, ok)
			require.Equal(t, tcpaddr.IP.To4(), srcAddr.IP)
			require.Equal(t, tcpaddr.Port, srcAddr.Port)
			dstAddr, ok := p.DstAddress.(*net.TCPAddr)
			require.True(t, ok)
			require.Equal(t, tcpaddr.IP.To4(), dstAddr.IP)
			require.Equal(t, tcpaddr.Port, dstAddr.Port)
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

func TestMixIPv4AndIPv6ProxyToBytes(t *testing.T) {
	tests := []struct {
		srcIP     net.IP
		dstIP     net.IP
		srcPort   int
		dstPort   int
		wantAF    ProxyAddressFamily
		wantIPLen int
	}{
		{
			srcIP:     net.ParseIP("192.168.1.1"),
			dstIP:     net.ParseIP("192.168.1.2"),
			srcPort:   1234,
			dstPort:   5678,
			wantAF:    ProxyAFINet,
			wantIPLen: net.IPv4len,
		},
		{
			srcIP:     net.ParseIP("2001:db8::1"),
			dstIP:     net.ParseIP("2001:db8::2"),
			srcPort:   1234,
			dstPort:   5678,
			wantAF:    ProxyAFINet6,
			wantIPLen: net.IPv6len,
		},
		{
			srcIP:     net.ParseIP("192.168.1.1"),
			dstIP:     net.ParseIP("2001:db8::1"),
			srcPort:   1234,
			dstPort:   5678,
			wantAF:    ProxyAFINet6,
			wantIPLen: net.IPv6len,
		},
		{
			srcIP:     net.ParseIP("192.168.1.1"),
			dstIP:     net.ParseIP("::ffff:192.168.1.2"),
			srcPort:   1234,
			dstPort:   5678,
			wantAF:    ProxyAFINet,
			wantIPLen: net.IPv4len,
		},
		{
			srcIP:     net.ParseIP("::ffff:192.168.1.1"),
			dstIP:     net.ParseIP("::ffff:192.168.1.2"),
			srcPort:   1234,
			dstPort:   5678,
			wantAF:    ProxyAFINet,
			wantIPLen: net.IPv4len,
		},
		{
			srcIP:     net.ParseIP("::ffff:192.168.1.1"),
			dstIP:     net.ParseIP("2001:db8::1"),
			srcPort:   1234,
			dstPort:   5678,
			wantAF:    ProxyAFINet6,
			wantIPLen: net.IPv6len,
		},
	}

	for _, tt := range tests {
		hdr := &Proxy{
			Version:    ProxyVersion2,
			Command:    ProxyCommandProxy,
			SrcAddress: &net.TCPAddr{IP: tt.srcIP, Port: tt.srcPort},
			DstAddress: &net.TCPAddr{IP: tt.dstIP, Port: tt.dstPort},
		}

		hdrBytes, err := hdr.ToBytes()
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(hdrBytes), len(MagicV2)+4)

		addressFamily := ProxyAddressFamily(hdrBytes[len(MagicV2)+1] >> 4)
		require.Equal(t, tt.wantAF, addressFamily)

		length := int(hdrBytes[len(MagicV2)+2])<<8 | int(hdrBytes[len(MagicV2)+3])
		require.Equal(t, len(hdrBytes)-4-len(MagicV2), length)

		expectedPayloadSize := tt.wantIPLen*2 + 4
		require.Equal(t, expectedPayloadSize, length)
	}
}
