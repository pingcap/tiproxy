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

package backend

import (
	"crypto/tls"
	"net"
	"testing"

	"github.com/pingcap/TiProxy/lib/util/security"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
)

type tcpConnSuite struct {
	backendListener  net.Listener
	proxyListener    net.Listener
	backendTLSConfig *tls.Config
	clientTLSConfig  *tls.Config
	backendIO        *pnet.PacketIO
	proxyBIO         *pnet.PacketIO
	proxyCIO         *pnet.PacketIO
	clientIO         *pnet.PacketIO
}

func newTCPConnSuite(t *testing.T) *tcpConnSuite {
	var err error

	r := &tcpConnSuite{}

	r.backendListener, err = net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	r.proxyListener, err = net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	r.backendTLSConfig, r.clientTLSConfig, err = security.CreateTLSConfigForTest()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, r.backendListener.Close())
		require.NoError(t, r.proxyListener.Close())
	})

	return r
}

func (tc *tcpConnSuite) newConn(t *testing.T, enableRoute bool) func() {
	var wg waitgroup.WaitGroup
	if !enableRoute {
		wg.Run(func() {
			conn, err := tc.backendListener.Accept()
			require.NoError(t, err)
			tc.backendIO = pnet.NewPacketIO(conn)
		})
	}
	wg.Run(func() {
		if !enableRoute {
			backendConn, err := net.Dial("tcp", tc.backendListener.Addr().String())
			require.NoError(t, err)
			tc.proxyBIO = pnet.NewPacketIO(backendConn)
		}
		clientConn, err := tc.proxyListener.Accept()
		require.NoError(t, err)
		tc.proxyCIO = pnet.NewPacketIO(clientConn)
	})
	wg.Run(func() {
		conn, err := net.Dial("tcp", tc.proxyListener.Addr().String())
		require.NoError(t, err)
		tc.clientIO = pnet.NewPacketIO(conn)
	})
	wg.Wait()
	return func() {
		//  may be closed twice
		_ = tc.clientIO.Close()
		_ = tc.proxyCIO.Close()
		if tc.proxyBIO != nil {
			_ = tc.proxyBIO.Close()
		}
		if tc.backendIO != nil {
			_ = tc.backendIO.Close()
		}
	}
}

func (tc *tcpConnSuite) run(clientRunner, backendRunner func(*pnet.PacketIO) error, proxyRunner func(*pnet.PacketIO, *pnet.PacketIO) error) (cerr, berr, perr error) {
	var wg waitgroup.WaitGroup
	if clientRunner != nil {
		wg.Run(func() {
			cerr = clientRunner(tc.clientIO)
			if cerr != nil {
				_ = tc.clientIO.Close()
			}
		})
	}
	if backendRunner != nil {
		wg.Run(func() {
			berr = backendRunner(tc.backendIO)
			if berr != nil {
				_ = tc.backendIO.Close()
			}
		})
	}
	if proxyRunner != nil {
		wg.Run(func() {
			perr = proxyRunner(tc.proxyCIO, tc.proxyBIO)
			if perr != nil {
				_ = tc.proxyCIO.Close()
				if tc.proxyBIO != nil {
					_ = tc.proxyBIO.Close()
				}
			}
		})
	}
	wg.Wait()
	return
}
