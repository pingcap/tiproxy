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

	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/TiProxy/pkg/util/security"
	"github.com/pingcap/tidb/util"
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

func (tc *tcpConnSuite) newConn(t *testing.T) func() {
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		conn, err := tc.backendListener.Accept()
		require.NoError(t, err)
		tc.backendIO = pnet.NewPacketIO(conn)
	})
	wg.Run(func() {
		backendConn, err := net.Dial("tcp", tc.backendListener.Addr().String())
		require.NoError(t, err)
		tc.proxyBIO = pnet.NewPacketIO(backendConn)

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
		//  may  be closed twice
		_ = tc.clientIO.Close()
		_ = tc.proxyCIO.Close()
		_ = tc.proxyBIO.Close()
		_ = tc.backendIO.Close()
	}
}

func (tc *tcpConnSuite) run(t *testing.T, clientRunner, backendRunner func(*pnet.PacketIO) error, proxyRunner func(*pnet.PacketIO, *pnet.PacketIO) error) (cerr, berr, perr error) {
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		if clientRunner != nil {
			cerr = clientRunner(tc.clientIO)
			if cerr != nil {
				require.NoError(t, tc.clientIO.Close())
			}
		}
	})
	wg.Run(func() {
		if backendRunner != nil {
			berr = backendRunner(tc.backendIO)
			if berr != nil {
				require.NoError(t, tc.backendIO.Close())
			}
		}
	})
	wg.Run(func() {
		if proxyRunner != nil {
			perr = proxyRunner(tc.proxyCIO, tc.proxyBIO)
			if perr != nil {
				require.NoError(t, tc.proxyCIO.Close())
				require.NoError(t, tc.proxyBIO.Close())
			}
		}
	})
	wg.Wait()
	return
}
