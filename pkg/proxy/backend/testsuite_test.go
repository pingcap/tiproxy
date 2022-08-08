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
	"strings"
	"testing"

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
	mockCmdStr   = "str"
	mockCmdInt   = 100
	mockCmdByte  = byte(1)
	mockCmdBytes = []byte("01234567890123456789")
)

type testConfig struct {
	clientConfig  clientConfig
	proxyConfig   proxyConfig
	backendConfig backendConfig
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
	ts.mp = newMockProxy(&cfg.proxyConfig)
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

// Test forwarding commands between the client and the server.
func (ts *testSuite) executeCmd(t *testing.T) {
	cerr, berr, perr := ts.tc.run(t, ts.mc.request, ts.mb.respond, ts.mp.processCmd)
	require.NoError(t, berr)
	require.NoError(t, cerr)
	require.NoError(t, perr)
}
