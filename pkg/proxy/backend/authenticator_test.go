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
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/TiProxy/pkg/util/security"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func TestTLSConnection(t *testing.T) {
	backendTLSConfig, clientTLSConfig, err := security.CreateTLSConfigForTest()
	require.NoError(t, err)
	// Test combinations of (client or backend) * (enable or disable ClientSSL).
	for _, clientCap := range []uint32{defaultClientCapability & ^mysql.ClientSSL, defaultClientCapability | mysql.ClientSSL} {
		for _, backendCap := range []uint32{defaultBackendCapability & ^mysql.ClientSSL, defaultBackendCapability | mysql.ClientSSL} {
			msg := fmt.Sprintf("client cap: %d, backend cap: %d", clientCap, backendCap)
			cfg := newTestConfig()
			cfg.clientConfig.capability = clientCap
			cfg.setTLSConfig(clientTLSConfig, backendTLSConfig)
			cfg.backendConfig.capability = backendCap
			ts := newTestSuite(t, cfg)
			clientErr, proxyErr, backendErr := ts.authenticateFirstTime()
			if backendCap&mysql.ClientSSL == 0 {
				require.ErrorContains(t, proxyErr, "must enable TLS", msg)
			} else {
				require.NoError(t, clientErr, msg)
				require.NoError(t, proxyErr, msg)
				require.NoError(t, backendErr, msg)
			}
			ts.close()
		}
	}
}

func TestAuthPlugin(t *testing.T) {
	backendTLSConfig, clientTLSConfig, err := security.CreateTLSConfigForTest()
	require.NoError(t, err)
	cfgs := [][]cfgOverrider{
		{
			func(cfg *testConfig) {
				cfg.clientConfig.authData = mockAuthData
			},
			func(cfg *testConfig) {
				cfg.clientConfig.authData = []byte(strings.Repeat("x", 512))
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultClientCapability & ^mysql.ClientPluginAuth
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultClientCapability | mysql.ClientPluginAuth
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.authPlugin = mysql.AuthNativePassword
			},
			func(cfg *testConfig) {
				cfg.clientConfig.authPlugin = mysql.AuthCachingSha2Password
			},
		},
		{
			func(cfg *testConfig) {
				cfg.backendConfig.authPlugin = mysql.AuthNativePassword
			},
			func(cfg *testConfig) {
				cfg.backendConfig.authPlugin = mysql.AuthCachingSha2Password
			},
		},
		{
			func(cfg *testConfig) {
				cfg.backendConfig.switchAuth = true
			},
			func(cfg *testConfig) {
				cfg.backendConfig.switchAuth = false
			},
		},
		{
			func(cfg *testConfig) {
				cfg.backendConfig.authSucceed = true
			},
			func(cfg *testConfig) {
				cfg.backendConfig.authSucceed = false
			},
		},
	}

	// Get a combination of M^N configurations.
	cfgOverriders := getCfgCombinations(cfgs)
	for _, cfgs := range cfgOverriders {
		cfg := newTestConfig(cfgs...)
		cfg.setTLSConfig(clientTLSConfig, backendTLSConfig)
		ts := newTestSuite(t, cfg)
		clientErr, proxyErr, backendErr := ts.authenticateFirstTime()
		require.NoError(t, clientErr)
		require.NoError(t, proxyErr)
		require.NoError(t, backendErr)
		ts.close()
	}
}

func TestCapability(t *testing.T) {
	backendTLSConfig, clientTLSConfig, err := security.CreateTLSConfigForTest()
	require.NoError(t, err)
	cfgs := [][]cfgOverrider{
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultClientCapability & ^mysql.ClientProtocol41
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultClientCapability | mysql.ClientProtocol41
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultClientCapability & ^mysql.ClientConnectWithDB
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultClientCapability | mysql.ClientConnectWithDB
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultClientCapability & ^mysql.ClientConnectAtts
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultClientCapability | mysql.ClientConnectAtts
				cfg.clientConfig.attrs = []byte(strings.Repeat("x", 512))
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultClientCapability & ^mysql.ClientSecureConnection
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultClientCapability | mysql.ClientSecureConnection
			},
		},
	}

	// Get a combination of M^N configurations.
	cfgOverriders := getCfgCombinations(cfgs)
	for _, cfgs := range cfgOverriders {
		cfg := newTestConfig(cfgs...)
		cfg.setTLSConfig(clientTLSConfig, backendTLSConfig)
		ts := newTestSuite(t, cfg)
		clientErr, proxyErr, backendErr := ts.authenticateFirstTime()
		require.NoError(t, clientErr)
		require.NoError(t, proxyErr)
		require.NoError(t, backendErr)
		ts.close()
	}
}

func TestSecondHandshake(t *testing.T) {
	backendTLSConfig, clientTLSConfig, err := security.CreateTLSConfigForTest()
	require.NoError(t, err)

	hooks := []func(ts *testSuite){
		// Do nothing.
		func(ts *testSuite) {},
		// Test COM_INIT_DB and `use db`.
		func(ts *testSuite) {
			ts.changeDB("another_db")
		},
		// Test COM_CHANGE_USER.
		func(ts *testSuite) {
			ts.changeUser("another_user", "another_db")
		},
	}
	for _, hook := range hooks {
		cfg := newTestConfig()
		cfg.setTLSConfig(clientTLSConfig, backendTLSConfig)
		ts := newTestSuite(t, cfg)
		clientErr, proxyErr, backendErr := ts.authenticateFirstTime()
		require.NoError(t, clientErr)
		require.NoError(t, proxyErr)
		require.NoError(t, backendErr)
		// Call the hook after first handshake.
		hook(ts)
		proxyErr, backendErr = ts.authenticateSecondTime()
		require.NoError(t, proxyErr)
		require.NoError(t, backendErr)
		ts.close()
	}
}

func (ts *testSuite) authenticateFirstTime() (clientErr, proxyErr, backendErr error) {
	var wg util.WaitGroupWrapper
	clientErrCh, proxyErrCh, backendErrCh := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	ts.mb.authenticate(&wg, backendErrCh)
	ts.mp.authenticateFirstTime(&wg, ts.mb.serverAddr(), proxyErrCh)
	ts.mc.authenticate(&wg, ts.mp.serverAddr(), clientErrCh)
	// When any of the processes completes, its connection will be closed and thus other processes will quit.
	wg.Wait()
	select {
	case clientErr = <-clientErrCh:
	default:
	}
	select {
	case proxyErr = <-proxyErrCh:
	default:
	}
	select {
	case backendErr = <-backendErrCh:
	default:
	}
	// Check the data received by client equals to the data sent from the server and vice versa.
	if proxyErr == nil {
		require.Equal(ts.t, ts.mb.authSucceed, ts.mc.authSucceed)
		require.Equal(ts.t, ts.mc.username, ts.mb.username)
		require.Equal(ts.t, ts.mc.dbName, ts.mb.db)
		require.Equal(ts.t, ts.mc.authData, ts.mb.authData)
		require.Equal(ts.t, ts.mc.attrs, ts.mb.attrs)
	}
	return
}

// This must be called after authenticateFirstTime.
func (ts *testSuite) authenticateSecondTime() (proxyErr, backendErr error) {
	// Get a clone of the config for the backend.
	backendConfig := *ts.mb.backendConfig
	backendConfig.switchAuth = false
	backendConfig.authSucceed = true
	// The previous ts.mb should be closed after it's finished.
	ts.mb = newMockBackend(&backendConfig)
	var wg util.WaitGroupWrapper
	proxyErrCh, backendErrCh := make(chan error, 1), make(chan error, 1)
	ts.mb.authenticate(&wg, backendErrCh)
	ts.mp.authenticateSecondTime(&wg, ts.mb.serverAddr(), proxyErrCh)
	// When any of the processes completes, its connection will be closed and thus other processes will quit.
	wg.Wait()
	select {
	case proxyErr = <-proxyErrCh:
	default:
	}
	select {
	case backendErr = <-backendErrCh:
	default:
	}
	// Check the data of the proxy equals to the data received by the server.
	if proxyErr == nil {
		require.Equal(ts.t, ts.mc.username, ts.mb.username)
		require.Equal(ts.t, ts.mc.dbName, ts.mb.db)
		require.Equal(ts.t, []byte(ts.mp.sessionToken), ts.mb.authData)
	}
	return
}

func (mc *mockClient) authenticate(wg *util.WaitGroupWrapper, addr string, errCh chan error) {
	wg.Run(func() {
		var err error
		defer func() {
			if err != nil {
				errCh <- err
			}
			_ = mc.close()
		}()
		if err = mc.connect(addr); err != nil {
			return
		}
		if _, err = mc.ReadPacket(); err != nil {
			return
		}

		var resp []byte
		var headerPos int
		if mc.capability&mysql.ClientProtocol41 > 0 {
			resp, headerPos = pnet.GetNewVersionHandshakeResponse(mc.username, mc.dbName, mc.authPlugin, mc.collation, mc.authData, mc.attrs, mc.capability)
		} else {
			resp, headerPos = pnet.GetOldVersionHandshakeResponse(mc.username, mc.dbName, mc.authData, mc.capability)
		}
		if mc.capability&mysql.ClientSSL > 0 {
			if err = mc.WritePacket(resp[:headerPos], true); err != nil {
				return
			}
			if err = mc.UpgradeToClientTLS(mc.tlsConfig); err != nil {
				return
			}
		}
		if err = mc.WritePacket(resp, true); err != nil {
			return
		}
		err = mc.writePassword()
	})
}

func (mc *mockClient) writePassword() error {
	for {
		serverPkt, err := mc.ReadPacket()
		if err != nil {
			return err
		}
		switch serverPkt[0] {
		case mysql.OKHeader:
			mc.authSucceed = true
			return nil
		case mysql.ErrHeader:
			mc.authSucceed = false
			return nil
		case mysql.AuthSwitchRequest, pnet.ShaCommand:
			if err := mc.WritePacket(mc.authData, true); err != nil {
				return err
			}
		}
	}
}

func (mp *mockProxy) authenticateFirstTime(wg *util.WaitGroupWrapper, addr string, errCh chan error) {
	// Listen synchronously so that we can know the actual address immediately after calling this function.
	if err := mp.listen(); err != nil {
		errCh <- err
		return
	}
	wg.Run(func() {
		var err error
		defer func() {
			if err != nil {
				errCh <- err
			}
			_ = mp.close()
		}()
		if err = mp.connect(addr); err != nil {
			return
		}
		if err = mp.accept(); err != nil {
			return
		}
		if _, err = mp.auth.handshakeFirstTime(mp.tcpServer.PacketIO, mp.tcpClient.PacketIO, mp.frontendTLSConfig, mp.backendTLSConfig); err != nil {
			return
		}
	})
}

func (mp *mockProxy) authenticateSecondTime(wg *util.WaitGroupWrapper, addr string, errCh chan error) {
	wg.Run(func() {
		var err error
		defer func() {
			if err != nil {
				errCh <- err
			}
			_ = mp.close()
		}()
		if err = mp.connect(addr); err != nil {
			return
		}
		err = mp.auth.handshakeSecondTime(mp.tcpClient.PacketIO, mp.sessionToken)
	})
}

func (mb *mockBackend) authenticate(wg *util.WaitGroupWrapper, errCh chan error) {
	// Listen synchronously so that we can know the actual address immediately after calling this function.
	if err := mb.listen(); err != nil {
		errCh <- err
		return
	}
	wg.Run(func() {
		var err error
		defer func() {
			if err != nil {
				errCh <- err
			}
			_ = mb.close()
		}()
		if err = mb.accept(); err != nil {
			return
		}
		// write initial handshake
		if err = mb.WriteInitialHandshake(mb.capability, mb.salt, mb.authPlugin); err != nil {
			return
		}
		// read the response
		var clientPkt []byte
		if clientPkt, err = mb.ReadPacket(); err != nil {
			return
		}
		// upgrade to TLS
		capability := binary.LittleEndian.Uint16(clientPkt[:2])
		sslEnabled := uint32(capability)&mysql.ClientSSL > 0 && mb.capability&mysql.ClientSSL > 0
		if sslEnabled {
			if _, err = mb.UpgradeToServerTLS(mb.tlsConfig); err != nil {
				return
			}
			// read the response again
			if clientPkt, err = mb.ReadPacket(); err != nil {
				return
			}
		}
		resp := pnet.ParseHandshakeResponse(clientPkt)
		mb.username = resp.User
		mb.db = resp.DB
		mb.authData = resp.AuthData
		mb.attrs = resp.Attrs
		// verify password
		err = mb.verifyPassword()
	})
}

func (mb *mockBackend) verifyPassword() error {
	if mb.switchAuth {
		var err error
		if err = mb.WriteSwitchRequest(mb.authPlugin, mb.salt); err != nil {
			return err
		}
		if mb.authData, err = mb.ReadPacket(); err != nil {
			return err
		}
		switch mb.authPlugin {
		case mysql.AuthCachingSha2Password:
			if err = mb.WriteShaCommand(); err != nil {
				return err
			}
			if mb.authData, err = mb.ReadPacket(); err != nil {
				return err
			}
		}
	}
	if mb.authSucceed {
		if err := mb.WriteOKPacket(); err != nil {
			return err
		}
	} else {
		if err := mb.WriteErrPacket(mysql.NewErr(mysql.ErrAccessDenied)); err != nil {
			return err
		}
	}
	return nil
}
