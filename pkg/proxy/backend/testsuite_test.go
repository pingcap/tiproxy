// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tiproxy/pkg/manager/router"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
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
	defaultTestClientCapability = pnet.ClientLongPassword | pnet.ClientFoundRows | pnet.ClientLongFlag |
		pnet.ClientConnectWithDB | pnet.ClientNoSchema | pnet.ClientODBC | pnet.ClientLocalFiles | pnet.ClientIgnoreSpace |
		pnet.ClientProtocol41 | pnet.ClientInteractive | pnet.ClientSSL | pnet.ClientIgnoreSigpipe |
		pnet.ClientTransactions | pnet.ClientReserved | pnet.ClientSecureConnection | pnet.ClientMultiStatements |
		pnet.ClientMultiResults | pnet.ClientPluginAuth | pnet.ClientConnectAttrs | pnet.ClientPluginAuthLenencClientData |
		pnet.ClientDeprecateEOF
	defaultTestBackendCapability = defaultTestClientCapability | pnet.ClientCompress | pnet.ClientZstdCompressionAlgorithm
)

var (
	mockUsername      = "test_user"
	mockDBName        = "test_db"
	mockSalt          = [20]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	mockAuthData      = []byte("123456")
	mockToken         = strings.Repeat("t", 512)
	mockCmdStr        = "str"
	mockCmdInt        = 100
	mockCmdBytes      = []byte("01234567890123456789")
	mockSessionStates = "{\"current-db\":\"test_db\"}"
	mockSessionToken  = "{\"username\": \"str\"}"
)

type testConfig struct {
	clientConfig    *clientConfig
	proxyConfig     *proxyConfig
	backendConfig   *backendConfig
	testSuiteConfig *testSuiteConfig
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
				newOverriders = append(newOverriders, newOverrider)
			}
		}
		cfgOverriders = newOverriders
	}
	return cfgOverriders
}

func newTestConfig(overriders ...cfgOverrider) *testConfig {
	cfg := &testConfig{
		clientConfig:    newClientConfig(),
		proxyConfig:     newProxyConfig(),
		backendConfig:   newBackendConfig(),
		testSuiteConfig: newTestSuiteConfig(),
	}
	for _, overrider := range overriders {
		if overrider != nil {
			overrider(cfg)
		}
	}
	return cfg
}

type testSuite struct {
	*testSuiteConfig
	tc *tcpConnSuite
	mb *mockBackend
	mp *mockProxy
	mc *mockClient
}

type testSuiteConfig struct {
	// When true, routing logic in handshakeFirstTime is enabled.
	// When false, a manual created backendIO is passed to handler to skip the routing logic.
	enableRouteLogic bool
}

func newTestSuiteConfig() *testSuiteConfig {
	return &testSuiteConfig{}
}

type checker func(t *testing.T, ts *testSuite)

func newTestSuite(t *testing.T, tc *tcpConnSuite, overriders ...cfgOverrider) (*testSuite, func()) {
	ts := &testSuite{}
	overriders = append([]cfgOverrider{func(config *testConfig) {
		config.backendConfig.tlsConfig = tc.backendTLSConfig
		config.proxyConfig.backendTLSConfig = tc.clientTLSConfig
		config.proxyConfig.frontendTLSConfig = tc.backendTLSConfig
		config.clientConfig.tlsConfig = tc.clientTLSConfig
		config.proxyConfig.handler.getRouter = func(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error) {
			return router.NewStaticRouter([]string{ts.tc.backendListener.Addr().String()}), nil
		}
	}}, overriders...)
	cfg := newTestConfig(overriders...)
	ts.mb = newMockBackend(cfg.backendConfig)
	ts.mp = newMockProxy(t, cfg.proxyConfig)
	ts.mc = newMockClient(cfg.clientConfig)
	ts.tc = tc
	ts.testSuiteConfig = cfg.testSuiteConfig
	clean := tc.newConn(t, ts.enableRouteLogic)
	return ts, clean
}

func (ts *testSuite) setConfig(overriders ...cfgOverrider) {
	cfg := newTestConfig(overriders...)
	ts.mb.backendConfig = cfg.backendConfig
	ts.mp.proxyConfig = cfg.proxyConfig
	ts.mc.clientConfig = cfg.clientConfig
}

func (ts *testSuite) changeDB(db string) {
	ts.mc.dbName = db
	ts.mp.authenticator.updateCurrentDB(db)
}

func (ts *testSuite) changeUser(username, db string) {
	ts.mc.username = username
	ts.mc.dbName = db
	req := &pnet.ChangeUserReq{
		User: username,
		DB:   db,
	}
	ts.mp.authenticator.changeUser(req)
}

func (ts *testSuite) runAndCheck(t *testing.T, c checker, clientRunner, backendRunner func(*pnet.PacketIO) error,
	proxyRunner func(*pnet.PacketIO, *pnet.PacketIO) error) {
	ts.mc.err, ts.mb.err, ts.mp.err = ts.tc.run(t, clientRunner, backendRunner, proxyRunner)
	if c == nil {
		require.NoError(t, ts.mc.err)
		require.NoError(t, ts.mb.err)
		if ts.mb.err != nil {
			require.True(t, pnet.IsMySQLError(ts.mp.err))
		}
		if clientRunner != nil && backendRunner != nil {
			// Ensure all the packets are forwarded.
			msg := fmt.Sprintf("cmd:%d responseType:%d", ts.mc.cmd, ts.mb.respondType)
			require.Equal(t, ts.tc.backendIO.GetSequence(), ts.tc.clientIO.GetSequence(), msg)
			require.Equal(t, ts.tc.clientIO.OutBytes(), ts.tc.proxyCIO.InBytes(), msg)
			require.Equal(t, ts.tc.clientIO.InBytes(), ts.tc.proxyCIO.OutBytes(), msg)
		}
	} else {
		c(t, ts)
	}
}

// The client connects to the backend through the proxy.
func (ts *testSuite) authenticateFirstTime(t *testing.T, c checker) {
	ts.runAndCheck(t, c, ts.mc.authenticate, ts.mb.authenticate, ts.mp.authenticateFirstTime)
	if c == nil {
		// Check the data received by client equals to the data sent from the server and vice versa.
		require.Equal(t, ts.mb.authSucceed, ts.mc.authSucceed)
		require.Equal(t, ts.mc.username, ts.mb.username)
		if ts.mc.capability&pnet.ClientConnectWithDB > 0 {
			require.Equal(t, ts.mc.dbName, ts.mb.db)
		}
		require.Equal(t, ts.mc.authData, ts.mb.authData)
		if ts.mc.capability&pnet.ClientConnectAttrs > 0 {
			require.Equal(t, ts.mc.attrs, ts.mb.attrs)
		}
		if !ts.mb.authSucceed {
			require.Equal(t, SrcClientAuthFail, Error2Source(ts.mp.err))
		}
	}
}

// The proxy reconnects to the proxy using preserved client data.
// This must be called after authenticateFirstTime.
func (ts *testSuite) authenticateSecondTime(t *testing.T, c checker) {
	ts.mb.backendConfig.authSucceed = true
	ts.tc.reconnectBackend(t)
	ts.runAndCheck(t, c, nil, ts.mb.authenticate, ts.mp.authenticateSecondTime)
	if c == nil {
		require.Equal(t, ts.mc.username, ts.mb.username)
		require.Equal(t, ts.mc.dbName, ts.mb.db)
		require.Equal(t, []byte(ts.mp.sessionToken), ts.mb.authData)
	}
}

// Test forwarding commands between the client and the server.
// It verifies that it won't hang or report errors, and all the packets are forwarded.
func (ts *testSuite) executeCmd(t *testing.T, c checker) {
	ts.runAndCheck(t, c, ts.mc.request, ts.mb.respond, ts.mp.processCmd)
}

// Execute multiple commands at once to reuse the same ComProcessor.
func (ts *testSuite) executeMultiCmd(t *testing.T, cfgs []cfgOverrider, c checker) {
	for _, cfg := range cfgs {
		ts.setConfig(cfg)
		ts.runAndCheck(t, nil, ts.mc.request, ts.mb.respond, ts.mp.processCmd)
	}
	// Only check it at last.
	if c != nil {
		c(t, ts)
	}
}

// Test querying from the backend directly.
// It verifies that it won't hang or panic, and column / row counts match.
func (ts *testSuite) query(t *testing.T, c checker) {
	ts.runAndCheck(t, c, nil, ts.mb.respond, ts.mp.directQuery)
	if c == nil {
		if ts.mb.respondType == responseTypeResultSet {
			require.Equal(t, ts.mb.columns, len(ts.mp.rs.Fields))
			require.Equal(t, ts.mb.rows, len(ts.mp.rs.RowDatas))
		}
	}
}
