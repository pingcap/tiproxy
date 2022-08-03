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
	"encoding/binary"
	"strings"
	"testing"

	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

// TestSuite creates a sql client and a sql server which connect to the proxy with TCP connection.
// Both the mocked client and server act like a real client and server: they parse data and
// respond to the proxy.
// We can test various client capabilities, auth-plugins, and TLS configs using the mocked client
// and server. We create an M^N combinations of the possibilities to test.
// We test the correctness by checking whether the data received by the client equals to the data
// sent from the server and vice versa.

const (
	defaultBackendCapability = mysql.ClientLongPassword | mysql.ClientLongFlag |
		mysql.ClientConnectWithDB | mysql.ClientProtocol41 | mysql.ClientSSL |
		mysql.ClientTransactions | mysql.ClientSecureConnection | mysql.ClientFoundRows |
		mysql.ClientMultiStatements | mysql.ClientMultiResults | mysql.ClientLocalFiles |
		mysql.ClientConnectAtts | mysql.ClientPluginAuth | mysql.ClientInteractive
	defaultClientCapability = defaultBackendCapability
)

var (
	mockUsername = "test_user"
	mockDBName   = "test_db"
	mockSalt     = []byte("01234567890123456789")
	mockAuthData = []byte("123456")
	mockToken    = strings.Repeat("t", 512)
)

type testConfig struct {
	clientConfig  clientConfig
	proxyConfig   proxyConfig
	backendConfig backendConfig
}

type clientConfig struct {
	tlsConfig  *tls.Config
	capability uint32
	username   string
	dbName     string
	collation  uint8
	authPlugin string
	authData   []byte
	attrs      []byte
}

type proxyConfig struct {
	frontendTLSConfig *tls.Config
	backendTLSConfig  *tls.Config
	sessionToken      string
}

type backendConfig struct {
	tlsConfig   *tls.Config
	capability  uint32
	salt        []byte
	authPlugin  string
	switchAuth  bool
	authSucceed bool
}

type cfgOverrider func(config *testConfig)

// Get a combination of M^N configurations from [N][M]cfgOverrider to test.
func getCfgCombinations(cfgs [][]cfgOverrider) [][]cfgOverrider {
	if len(cfgs) == 0 {
		return nil
	}
	cfgOverriders := make([][]cfgOverrider, 0, len(cfgs[0]))
	for _, cfg := range cfgs[0] {
		cfgOverriders = append(cfgOverriders, []cfgOverrider{cfg})
	}
	for _, cfgList := range cfgs[1:] {
		newOverriders := make([][]cfgOverrider, 0, len(cfgOverriders)*len(cfgList))
		// Append the cfg to each of the existing overrider list.
		for _, cfg := range cfgList {
			for _, o := range cfgOverriders {
				newOverrider := make([]cfgOverrider, 0, len(o)+1)
				newOverrider = append(newOverrider, o...)
				newOverrider = append(newOverrider, cfg)
				newOverriders = append(newOverriders, append(o, cfg))
			}
		}
		cfgOverriders = newOverriders
	}
	return cfgOverriders
}

func newTestConfig(overriders ...cfgOverrider) *testConfig {
	cfg := &testConfig{
		clientConfig: clientConfig{
			capability: defaultClientCapability,
			username:   mockUsername,
			dbName:     mockDBName,
			authPlugin: mysql.AuthCachingSha2Password,
			authData:   mockAuthData,
			attrs:      make([]byte, 0),
		},
		proxyConfig: proxyConfig{
			sessionToken: mockToken,
		},
		backendConfig: backendConfig{
			capability:  defaultBackendCapability,
			salt:        mockSalt,
			authPlugin:  mysql.AuthCachingSha2Password,
			switchAuth:  true,
			authSucceed: true,
		},
	}
	for _, overrider := range overriders {
		if overrider != nil {
			overrider(cfg)
		}
	}
	return cfg
}

type mockClient struct {
	// Inputs that assigned by the test and will be sent to the server.
	*clientConfig
	// Outputs that received from the server and will be checked by the test.
	authSucceed bool
}

func newMockClient(cfg *clientConfig) *mockClient {
	return &mockClient{
		clientConfig: cfg,
	}
}

func (mc *mockClient) authenticate(packetIO *pnet.PacketIO) error {
	if _, err := packetIO.ReadPacket(); err != nil {
		return err
	}

	var resp []byte
	var headerPos int
	if mc.capability&mysql.ClientProtocol41 > 0 {
		resp, headerPos = pnet.MakeNewVersionHandshakeResponse(mc.username, mc.dbName, mc.authPlugin, mc.collation, mc.authData, mc.attrs, mc.capability)
	} else {
		resp, headerPos = pnet.MakeOldVersionHandshakeResponse(mc.username, mc.dbName, mc.authData, mc.capability)
	}
	if mc.capability&mysql.ClientSSL > 0 {
		if err := packetIO.WritePacket(resp[:headerPos], true); err != nil {
			return err
		}
		if err := packetIO.UpgradeToClientTLS(mc.tlsConfig); err != nil {
			return err
		}
	}
	if err := packetIO.WritePacket(resp, true); err != nil {
		return err
	}
	return mc.writePassword(packetIO)
}

func (mc *mockClient) writePassword(packetIO *pnet.PacketIO) error {
	for {
		serverPkt, err := packetIO.ReadPacket()
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
			if err := packetIO.WritePacket(mc.authData, true); err != nil {
				return err
			}
		}
	}
}

type mockProxy struct {
	*proxyConfig
	auth *Authenticator
}

func newMockProxy(cfg *testConfig) *mockProxy {
	return &mockProxy{
		proxyConfig: &cfg.proxyConfig,
		auth:        new(Authenticator),
	}
}

func (mp *mockProxy) authenticateFirstTime(clientIO, backendIO *pnet.PacketIO) error {
	_, err := mp.auth.handshakeFirstTime(clientIO, backendIO, mp.frontendTLSConfig, mp.backendTLSConfig)
	return err
}

func (mp *mockProxy) authenticateSecondTime(_, backendIO *pnet.PacketIO) error {
	return mp.auth.handshakeSecondTime(backendIO, mp.sessionToken)
}

type mockBackend struct {
	// Inputs that assigned by the test and will be sent to the client.
	*backendConfig
	// Outputs that received from the client and will be checked by the test.
	username string
	authData []byte
	db       string
	attrs    []byte
}

func newMockBackend(cfg *backendConfig) *mockBackend {
	return &mockBackend{
		backendConfig: cfg,
	}
}

func (mb *mockBackend) authenticate(packetIO *pnet.PacketIO) error {
	var err error
	// write initial handshake
	if err = packetIO.WriteInitialHandshake(mb.capability, mb.salt, mb.authPlugin); err != nil {
		return err
	}
	// read the response
	var clientPkt []byte
	if clientPkt, err = packetIO.ReadPacket(); err != nil {
		return err
	}
	// upgrade to TLS
	capability := binary.LittleEndian.Uint16(clientPkt[:2])
	sslEnabled := uint32(capability)&mysql.ClientSSL > 0 && mb.capability&mysql.ClientSSL > 0
	if sslEnabled {
		if _, err = packetIO.UpgradeToServerTLS(mb.tlsConfig); err != nil {
			return err
		}
		// read the response again
		if clientPkt, err = packetIO.ReadPacket(); err != nil {
			return err
		}
	}
	resp := pnet.ParseHandshakeResponse(clientPkt)
	mb.username = resp.User
	mb.db = resp.DB
	mb.authData = resp.AuthData
	mb.attrs = resp.Attrs
	// verify password
	return mb.verifyPassword(packetIO)
}

func (mb *mockBackend) verifyPassword(packetIO *pnet.PacketIO) error {
	if mb.switchAuth {
		var err error
		if err = packetIO.WriteSwitchRequest(mb.authPlugin, mb.salt); err != nil {
			return err
		}
		if mb.authData, err = packetIO.ReadPacket(); err != nil {
			return err
		}
		switch mb.authPlugin {
		case mysql.AuthCachingSha2Password:
			if err = packetIO.WriteShaCommand(); err != nil {
				return err
			}
			if mb.authData, err = packetIO.ReadPacket(); err != nil {
				return err
			}
		}
	}
	if mb.authSucceed {
		if err := packetIO.WriteOKPacket(); err != nil {
			return err
		}
	} else {
		if err := packetIO.WriteErrPacket(mysql.NewErr(mysql.ErrAccessDenied)); err != nil {
			return err
		}
	}
	return nil
}

type testSuite struct {
	tc *tcpConnSuite
	mb *mockBackend
	mp *mockProxy
	mc *mockClient
}

type errChecker func(t *testing.T, ts *testSuite, cerr, berr, perr error)

func newTestSuite(t *testing.T, tc *tcpConnSuite, overriders ...cfgOverrider) (*testSuite, func()) {
	ts := &testSuite{}
	cfg := newTestConfig(append(overriders, func(config *testConfig) {
		config.backendConfig.tlsConfig = tc.backendTLSConfig
		config.proxyConfig.backendTLSConfig = tc.clientTLSConfig
		config.proxyConfig.frontendTLSConfig = tc.backendTLSConfig
		config.clientConfig.tlsConfig = tc.clientTLSConfig
	})...)
	ts.mb = newMockBackend(&cfg.backendConfig)
	ts.mp = newMockProxy(cfg)
	ts.mc = newMockClient(&cfg.clientConfig)
	ts.tc = tc
	clean := tc.newConn(t)
	return ts, clean
}

func (ts *testSuite) changeDB(db string) {
	ts.mc.dbName = db
	ts.mp.auth.updateCurrentDB(db)
}

func (ts *testSuite) changeUser(username, db string) {
	ts.mc.username = username
	ts.mc.dbName = db
	ts.mp.auth.changeUser(username, db)
}

// The client connects to the backend through the proxy.
func (ts *testSuite) authenticateFirstTime(t *testing.T, ce errChecker) {
	cerr, berr, perr := ts.tc.run(t, ts.mc.authenticate, ts.mb.authenticate, ts.mp.authenticateFirstTime)
	if ce == nil {
		require.NoError(t, berr)
		require.NoError(t, cerr)
		require.NoError(t, perr)
		// Check the data received by client equals to the data sent from the server and vice versa.
		require.Equal(t, ts.mb.authSucceed, ts.mc.authSucceed)
		require.Equal(t, ts.mc.username, ts.mb.username)
		require.Equal(t, ts.mc.dbName, ts.mb.db)
		require.Equal(t, ts.mc.authData, ts.mb.authData)
		require.Equal(t, ts.mc.attrs, ts.mb.attrs)
	} else {
		ce(t, ts, cerr, berr, perr)
	}
}

// The proxy reconnects to the proxy using preserved client data.
// This must be called after authenticateFirstTime.
func (ts *testSuite) authenticateSecondTime(t *testing.T, ce errChecker) {
	// The server won't request switching auth-plugin this time.
	ts.mb.backendConfig.switchAuth = false
	ts.mb.backendConfig.authSucceed = true
	cerr, berr, perr := ts.tc.run(t, nil, ts.mb.authenticate, ts.mp.authenticateSecondTime)
	if ce == nil {
		require.NoError(t, berr)
		require.NoError(t, cerr)
		require.NoError(t, perr)
		require.Equal(t, ts.mc.username, ts.mb.username)
		require.Equal(t, ts.mc.dbName, ts.mb.db)
		require.Equal(t, []byte(ts.mp.sessionToken), ts.mb.authData)
	} else {
		ce(t, ts, cerr, berr, perr)
	}
}
