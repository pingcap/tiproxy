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
	"net"
	"strings"
	"testing"

	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestUnsupportedCapability(t *testing.T) {
	cfgs := [][]cfgOverrider{
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability & ^mysql.ClientSSL
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability | mysql.ClientSSL
			},
		},
		{
			func(cfg *testConfig) {
				cfg.backendConfig.capability = defaultTestBackendCapability & ^mysql.ClientSSL
			},
			func(cfg *testConfig) {
				cfg.backendConfig.capability = defaultTestBackendCapability | mysql.ClientSSL
			},
		},
		{
			func(cfg *testConfig) {
				cfg.backendConfig.capability = defaultTestBackendCapability & ^mysql.ClientDeprecateEOF
			},
			func(cfg *testConfig) {
				cfg.backendConfig.capability = defaultTestBackendCapability | mysql.ClientDeprecateEOF
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability & ^mysql.ClientProtocol41
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability | mysql.ClientProtocol41
			},
		},
		{
			func(cfg *testConfig) {
				cfg.backendConfig.capability = defaultTestClientCapability & ^mysql.ClientPSMultiResults
			},
			func(cfg *testConfig) {
				cfg.backendConfig.capability = defaultTestClientCapability | mysql.ClientPSMultiResults
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability & ^mysql.ClientPSMultiResults
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability | mysql.ClientPSMultiResults
			},
		},
	}

	tc := newTCPConnSuite(t)
	cfgOverriders := getCfgCombinations(cfgs)
	for _, cfgs := range cfgOverriders {
		ts, clean := newTestSuite(t, tc, cfgs...)
		ts.authenticateFirstTime(t, func(t *testing.T, _ *testSuite) {
			if ts.mb.backendConfig.capability&defRequiredBackendCaps.Uint32() != defRequiredBackendCaps.Uint32() {
				require.ErrorIs(t, ts.mp.err, ErrCapabilityNegotiation)
			} else if ts.mc.clientConfig.capability&requiredFrontendCaps.Uint32() != requiredFrontendCaps.Uint32() {
				require.ErrorIs(t, ts.mp.err, ErrCapabilityNegotiation)
			} else {
				require.NoError(t, ts.mc.err)
				require.NoError(t, ts.mp.err)
				require.NoError(t, ts.mb.err)
			}
		})
		clean()
	}
}

func TestAuthPlugin(t *testing.T) {
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
				cfg.clientConfig.capability = defaultTestClientCapability & ^mysql.ClientPluginAuth
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability | mysql.ClientPluginAuth
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
				cfg.backendConfig.authSucceed = true
			},
			func(cfg *testConfig) {
				cfg.backendConfig.authSucceed = false
			},
		},
	}

	tc := newTCPConnSuite(t)
	cfgOverriders := getCfgCombinations(cfgs)
	for _, cfgs := range cfgOverriders {
		ts, clean := newTestSuite(t, tc, cfgs...)
		ts.authenticateFirstTime(t, nil)
		clean()
	}
}

func TestCapability(t *testing.T) {
	cfgs := [][]cfgOverrider{
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability & ^mysql.ClientConnectWithDB
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability | mysql.ClientConnectWithDB
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability & ^mysql.ClientConnectAtts
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability | mysql.ClientConnectAtts
				cfg.clientConfig.attrs = map[string]string{"key": "value"}
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability & ^mysql.ClientSecureConnection
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability | mysql.ClientSecureConnection
			},
		},
	}

	tc := newTCPConnSuite(t)
	cfgOverriders := getCfgCombinations(cfgs)
	for _, cfgs := range cfgOverriders {
		ts, clean := newTestSuite(t, tc, cfgs...)
		ts.authenticateFirstTime(t, nil)
		clean()
	}
}

func TestSecondHandshake(t *testing.T) {
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

	tc := newTCPConnSuite(t)
	for _, hook := range hooks {
		ts, clean := newTestSuite(t, tc)
		ts.authenticateFirstTime(t, nil)
		hook(ts)
		ts.authenticateSecondTime(t, nil)
		clean()
	}
}

func TestCustomAuth(t *testing.T) {
	tc := newTCPConnSuite(t)
	handler := &CustomHandshakeHandler{
		outUsername:   "rewritten_user",
		outAttrs:      map[string]string{"key": "value"},
		outCapability: SupportedServerCapabilities & ^pnet.ClientDeprecateEOF,
	}
	ts, clean := newTestSuite(t, tc, func(cfg *testConfig) {
		cfg.proxyConfig.handler = handler
	})
	checker := func() {
		require.Equal(t, ts.mc.username, handler.inUsername)
		require.Equal(t, handler.outUsername, ts.mb.username)
		require.Equal(t, handler.outAttrs, ts.mb.attrs)
		require.Equal(t, handler.outCapability&pnet.ClientDeprecateEOF, pnet.Capability(ts.mb.capability)&pnet.ClientDeprecateEOF)
		host, _, err := net.SplitHostPort(handler.inAddr)
		require.NoError(t, err)
		require.Equal(t, host, "::1")
	}
	ts.authenticateFirstTime(t, func(t *testing.T, ts *testSuite) {})
	checker()
	ts.authenticateSecondTime(t, func(t *testing.T, ts *testSuite) {})
	checker()
	clean()
}
