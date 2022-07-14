// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package net

import (
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProxy(t *testing.T) {
	tcpaddr, err := net.ResolveTCPAddr("tcp", "192.168.1.1:34")
	require.NoError(t, err)

	testConn(t,
		func(t *testing.T, cli *PacketIO) {
			require.NoError(t, cli.writeProxyV2(&Proxy{
				Version:    ProxyVersion2,
				Command:    ProxyCommandLocal,
				SrcAddress: tcpaddr,
				DstAddress: tcpaddr,
				TLV: []ProxyTlv{
					{
						typ:     ProxyTlvALPN,
						content: nil,
					},
					{
						typ:     ProxyTlvUniqueID,
						content: []byte("test"),
					},
				},
			}))
		},
		func(t *testing.T, srv *PacketIO) {
			var ok bool
			var err error


			// skip 4 bytes of magic
			var hdr [4]byte
			_, err = io.ReadFull(srv.buf, hdr[:])
			require.NoError(t, err)

			// try to parse V2
			ok, err = srv.parseProxyV2()
			require.NoError(t, err)
			require.True(t, ok)
			require.NotNil(t, srv.proxy)
			require.Equal(t, tcpaddr, srv.proxy.SrcAddress)
			require.Equal(t, tcpaddr, srv.proxy.DstAddress)
			require.Equal(t, ProxyVersion2, srv.proxy.Version)
			require.Equal(t, ProxyCommandLocal, srv.proxy.Command)
			require.Len(t, srv.proxy.TLV, 2)
			require.Equal(t, ProxyTlvALPN, srv.proxy.TLV[0].typ)
			require.Equal(t, ProxyTlvUniqueID, srv.proxy.TLV[1].typ)
			require.Equal(t, []byte("test"), srv.proxy.TLV[1].content)
		},
	)
}
