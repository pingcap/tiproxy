// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package proxyprotocol

import (
	"bytes"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/pingcap/tiproxy/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestProxyListener(t *testing.T) {
	tcpaddr, err := net.ResolveTCPAddr("tcp", "192.168.1.1:34")
	require.NoError(t, err)

	testkit.TestTCPConnWithListener(t,
		func(t *testing.T, network, addr string) net.Listener {
			ln, err := net.Listen(network, addr)
			require.NoError(t, err)
			return NewListener(ln)
		},
		func(t *testing.T, c net.Conn) {
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
			_, err = io.Copy(c, bytes.NewReader(b))
			require.NoError(t, err)
			_, err = io.Copy(c, strings.NewReader("test"))
			require.NoError(t, err)
		},
		func(t *testing.T, c net.Conn) {
			all, err := io.ReadAll(c)
			require.NoError(t, err)
			require.Equal(t, []byte("test"), all)
			require.Equal(t, tcpaddr.String(), c.RemoteAddr().String())
		}, 1)

	testkit.TestTCPConnWithListener(t,
		func(t *testing.T, network, addr string) net.Listener {
			ln, err := net.Listen(network, addr)
			require.NoError(t, err)
			return NewListener(ln)
		},
		func(t *testing.T, c net.Conn) {
			_, err = io.Copy(c, strings.NewReader("test"))
			require.NoError(t, err)
		},
		func(t *testing.T, c net.Conn) {
			all, err := io.ReadAll(c)
			require.NoError(t, err)
			require.Equal(t, []byte("test"), all)
		}, 1)
}
