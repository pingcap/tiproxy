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
	"net"
	"strings"
	"testing"

	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/TiProxy/pkg/util/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util"
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

func (cfg *testConfig) setTLSConfig(clientTLSConfig, backendTLSConfig *tls.Config) {
	cfg.clientConfig.tlsConfig = clientTLSConfig
	cfg.proxyConfig.frontendTLSConfig = backendTLSConfig
	cfg.proxyConfig.backendTLSConfig = clientTLSConfig
	cfg.backendConfig.tlsConfig = backendTLSConfig
}

type testSuite struct {
	t  *testing.T
	mb *mockBackend
	mp *mockProxy
	mc *mockClient
}

func newTestSuite(t *testing.T, cfg *testConfig) *testSuite {
	return &testSuite{
		t:  t,
		mb: newMockBackend(&cfg.backendConfig),
		mp: newMockProxy(cfg),
		mc: newMockClient(&cfg.clientConfig),
	}
}

// setup opens listeners. It's called only once for each test case.
func (ts *testSuite) setup() {
	require.NoError(ts.t, ts.mb.listen())
	require.NoError(ts.t, ts.mp.listen())
}

// reset closes connections but keep listeners opening. It's called for each handshake.
func (ts *testSuite) reset(cfg *testConfig) {
	require.NoError(ts.t, ts.mc.cleanup())
	require.NoError(ts.t, ts.mp.cleanup())
	require.NoError(ts.t, ts.mb.cleanup())
	ts.mc.clientConfig = &cfg.clientConfig
	ts.mp.proxyConfig = &cfg.proxyConfig
	ts.mb.backendConfig = &cfg.backendConfig
}

func (ts *testSuite) authenticateFirstTime() (clientErr, proxyErr, backendErr error) {
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		require.NoError(ts.t, ts.mb.accept())
		backendErr = ts.mb.authenticate()
		require.NoError(ts.t, ts.mb.cleanup())
	})
	wg.Run(func() {
		require.NoError(ts.t, ts.mp.connect(ts.mb.serverAddr()))
		require.NoError(ts.t, ts.mp.accept())
		proxyErr = ts.mp.authenticateFirstTime()
		require.NoError(ts.t, ts.mp.cleanup())
	})
	wg.Run(func() {
		require.NoError(ts.t, ts.mc.connect(ts.mp.serverAddr()))
		clientErr = ts.mc.authenticate()
		require.NoError(ts.t, ts.mc.cleanup())
	})
	wg.Wait()
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
	// The server won't request switching auth-plugin this time.
	ts.mb.backendConfig.switchAuth = false
	ts.mb.backendConfig.authSucceed = true
	// Only connect to the backend.
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		require.NoError(ts.t, ts.mb.accept())
		backendErr = ts.mb.authenticate()
		require.NoError(ts.t, ts.mb.cleanup())
	})
	wg.Run(func() {
		require.NoError(ts.t, ts.mp.connect(ts.mb.serverAddr()))
		proxyErr = ts.mp.authenticateSecondTime()
		require.NoError(ts.t, ts.mp.cleanup())
	})
	wg.Wait()
	// Check the data of the proxy equals to the data received by the server.
	if proxyErr == nil {
		require.Equal(ts.t, ts.mc.username, ts.mb.username)
		require.Equal(ts.t, ts.mc.dbName, ts.mb.db)
		require.Equal(ts.t, []byte(ts.mp.sessionToken), ts.mb.authData)
	}
	return
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

func (ts *testSuite) close() {
	require.NoError(ts.t, ts.mc.close())
	require.NoError(ts.t, ts.mp.close())
	require.NoError(ts.t, ts.mb.close())
}

type tcpClient struct {
	*pnet.PacketIO
}

func (c *tcpClient) connect(addr string) error {
	cn, err := net.Dial("tcp", addr)
	if err != nil {
		return errors.New("dial error")
	}
	c.PacketIO = pnet.NewPacketIO(cn)
	return nil
}

func (c *tcpClient) cleanup() error {
	if c.PacketIO != nil {
		err := c.PacketIO.Close()
		c.PacketIO = nil
		return err
	}
	return nil
}

func (c *tcpClient) close() error {
	return c.cleanup()
}

type tcpServer struct {
	*pnet.PacketIO
	listener net.Listener
}

func (s *tcpServer) listen() error {
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return err
	}
	s.listener = listener
	return nil
}

func (s *tcpServer) accept() error {
	conn, err := s.listener.Accept()
	if err != nil {
		return err
	}
	s.PacketIO = pnet.NewPacketIO(conn)
	return nil
}

func (s *tcpServer) serverAddr() string {
	return s.listener.Addr().String()
}

func (s *tcpServer) cleanup() error {
	var err error
	if s.PacketIO != nil {
		err = s.PacketIO.Close()
		s.PacketIO = nil
	}
	return err
}

func (s *tcpServer) close() error {
	var err1, err2 error
	err1 = s.cleanup()
	if s.listener != nil {
		err2 = s.listener.Close()
		s.listener = nil
	}
	if err1 != nil {
		return err1
	}
	return err2
}

type mockClient struct {
	tcpClient
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

func (mc *mockClient) authenticate() error {
	if _, err := mc.ReadPacket(); err != nil {
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
		if err := mc.WritePacket(resp[:headerPos], true); err != nil {
			return err
		}
		if err := mc.UpgradeToClientTLS(mc.tlsConfig); err != nil {
			return err
		}
	}
	if err := mc.WritePacket(resp, true); err != nil {
		return err
	}
	return mc.writePassword()
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

type mockProxy struct {
	tcpServer
	tcpClient
	*proxyConfig
	auth *Authenticator
}

func newMockProxy(cfg *testConfig) *mockProxy {
	return &mockProxy{
		proxyConfig: &cfg.proxyConfig,
		auth:        new(Authenticator),
	}
}

func (mp *mockProxy) authenticateFirstTime() error {
	_, err := mp.auth.handshakeFirstTime(mp.tcpServer.PacketIO, mp.tcpClient.PacketIO, mp.frontendTLSConfig, mp.backendTLSConfig)
	return err
}

func (mp *mockProxy) authenticateSecondTime() error {
	return mp.auth.handshakeSecondTime(mp.tcpClient.PacketIO, mp.sessionToken)
}

func (mp *mockProxy) cleanup() error {
	err1 := mp.tcpClient.cleanup()
	err2 := mp.tcpServer.cleanup()
	if err1 != nil {
		return err1
	}
	return err2
}

func (mp *mockProxy) close() error {
	err1 := mp.tcpClient.close()
	err2 := mp.tcpServer.close()
	if err1 != nil {
		return err1
	}
	return err2
}

type mockBackend struct {
	tcpServer
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

func (mb *mockBackend) authenticate() error {
	var err error
	// write initial handshake
	if err = mb.WriteInitialHandshake(mb.capability, mb.salt, mb.authPlugin); err != nil {
		return err
	}
	// read the response
	var clientPkt []byte
	if clientPkt, err = mb.ReadPacket(); err != nil {
		return err
	}
	// upgrade to TLS
	capability := binary.LittleEndian.Uint16(clientPkt[:2])
	sslEnabled := uint32(capability)&mysql.ClientSSL > 0 && mb.capability&mysql.ClientSSL > 0
	if sslEnabled {
		if _, err = mb.UpgradeToServerTLS(mb.tlsConfig); err != nil {
			return err
		}
		// read the response again
		if clientPkt, err = mb.ReadPacket(); err != nil {
			return err
		}
	}
	resp := pnet.ParseHandshakeResponse(clientPkt)
	mb.username = resp.User
	mb.db = resp.DB
	mb.authData = resp.AuthData
	mb.attrs = resp.Attrs
	// verify password
	return mb.verifyPassword()
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
