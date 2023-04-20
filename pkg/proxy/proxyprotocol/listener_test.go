// Copyright 2023 PingCAP, Inc.
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

package proxyprotocol

import (
	"bytes"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/pingcap/TiProxy/pkg/testkit"
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
						typ:     ProxyTlvALPN,
						content: nil,
					},
					{
						typ:     ProxyTlvUniqueID,
						content: []byte("test"),
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
