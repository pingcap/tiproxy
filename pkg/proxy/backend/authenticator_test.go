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

	"github.com/pingcap/TiProxy/pkg/util/security"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestTLSConnection(t *testing.T) {
	backendTLSConfig, clientTLSConfig, err := security.CreateTLSConfigForTest()
	require.NoError(t, err)
	cfgs := [][]cfgOverrider{
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultClientCapability & ^mysql.ClientSSL
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultClientCapability | mysql.ClientSSL
			},
		},
		{
			func(cfg *testConfig) {
				cfg.backendConfig.capability = defaultBackendCapability & ^mysql.ClientSSL
			},
			func(cfg *testConfig) {
				cfg.backendConfig.capability = defaultBackendCapability | mysql.ClientSSL
			},
		},
	}

	cfgOverriders := getCfgCombinations(cfgs)
	cfg := newTestConfig()
	ts := newTestSuite(t, cfg)
	ts.setup()
	for _, cfgs := range cfgOverriders {
		cfg := newTestConfig(cfgs...)
		cfg.setTLSConfig(clientTLSConfig, backendTLSConfig)
		ts.reset(cfg)
		clientErr, proxyErr, backendErr := ts.authenticateFirstTime()
		if cfg.backendConfig.capability&mysql.ClientSSL == 0 {
			require.ErrorContains(t, proxyErr, "must enable TLS")
		} else {
			require.NoError(t, clientErr)
			require.NoError(t, proxyErr)
			require.NoError(t, backendErr)
		}
	}
	ts.close()
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

	cfgOverriders := getCfgCombinations(cfgs)
	cfg := newTestConfig()
	ts := newTestSuite(t, cfg)
	ts.setup()
	for _, cfgs := range cfgOverriders {
		cfg := newTestConfig(cfgs...)
		cfg.setTLSConfig(clientTLSConfig, backendTLSConfig)
		ts.reset(cfg)
		clientErr, proxyErr, backendErr := ts.authenticateFirstTime()
		require.NoError(t, clientErr)
		require.NoError(t, proxyErr)
		require.NoError(t, backendErr)
	}
	ts.close()
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

	cfgOverriders := getCfgCombinations(cfgs)
	cfg := newTestConfig()
	ts := newTestSuite(t, cfg)
	ts.setup()
	for _, cfgs := range cfgOverriders {
		cfg := newTestConfig(cfgs...)
		cfg.setTLSConfig(clientTLSConfig, backendTLSConfig)
		ts.reset(cfg)
		clientErr, proxyErr, backendErr := ts.authenticateFirstTime()
		require.NoError(t, clientErr)
		require.NoError(t, proxyErr)
		require.NoError(t, backendErr)
	}
	ts.close()
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

	cfg := newTestConfig()
	ts := newTestSuite(t, cfg)
	ts.setup()
	for _, hook := range hooks {
		cfg := newTestConfig()
		cfg.setTLSConfig(clientTLSConfig, backendTLSConfig)
		ts.reset(cfg)
		clientErr, proxyErr, backendErr := ts.authenticateFirstTime()
		require.NoError(t, clientErr)
		require.NoError(t, proxyErr)
		require.NoError(t, backendErr)
		// Call the hook after first handshake.
		hook(ts)
		proxyErr, backendErr = ts.authenticateSecondTime()
		require.NoError(t, proxyErr)
		require.NoError(t, backendErr)
	}
	ts.close()
}
