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

	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestUnsupportedCapability(t *testing.T) {
	cfgs := [][]cfgOverrider{
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability & ^pnet.ClientSSL
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability | pnet.ClientSSL
			},
		},
		{
			func(cfg *testConfig) {
				cfg.backendConfig.capability = defaultTestBackendCapability & ^pnet.ClientSSL
			},
			func(cfg *testConfig) {
				cfg.backendConfig.capability = defaultTestBackendCapability | pnet.ClientSSL
			},
		},
		{
			func(cfg *testConfig) {
				cfg.backendConfig.capability = defaultTestBackendCapability & ^pnet.ClientDeprecateEOF
			},
			func(cfg *testConfig) {
				cfg.backendConfig.capability = defaultTestBackendCapability | pnet.ClientDeprecateEOF
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability & ^pnet.ClientProtocol41
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability | pnet.ClientProtocol41
			},
		},
		{
			func(cfg *testConfig) {
				cfg.backendConfig.capability = defaultTestClientCapability & ^pnet.ClientPSMultiResults
			},
			func(cfg *testConfig) {
				cfg.backendConfig.capability = defaultTestClientCapability | pnet.ClientPSMultiResults
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability & ^pnet.ClientPSMultiResults
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability | pnet.ClientPSMultiResults
			},
		},
	}

	tc := newTCPConnSuite(t)
	cfgOverriders := getCfgCombinations(cfgs)
	for _, cfgs := range cfgOverriders {
		ts, clean := newTestSuite(t, tc, cfgs...)
		ts.authenticateFirstTime(t, func(t *testing.T, _ *testSuite) {
			if ts.mb.backendConfig.capability&defRequiredBackendCaps != defRequiredBackendCaps {
				require.ErrorIs(t, ts.mp.err, ErrCapabilityNegotiation)
			} else if ts.mc.clientConfig.capability&requiredFrontendCaps != requiredFrontendCaps {
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
				cfg.clientConfig.capability = defaultTestClientCapability & ^pnet.ClientPluginAuth
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability | pnet.ClientPluginAuth
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
				cfg.clientConfig.capability = defaultTestClientCapability & ^pnet.ClientConnectWithDB
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability | pnet.ClientConnectWithDB
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability & ^pnet.ClientConnectAttrs
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability | pnet.ClientConnectAttrs
				cfg.clientConfig.attrs = map[string]string{"key": "value"}
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability & ^pnet.ClientSecureConnection
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestClientCapability | pnet.ClientSecureConnection
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
	reUser := "rewritten_user"
	reAttrs := map[string]string{"key": "value"}
	reCap := SupportedServerCapabilities & ^pnet.ClientDeprecateEOF
	inUser := ""
	ts, clean := newTestSuite(t, tc, func(cfg *testConfig) {
		handler := cfg.proxyConfig.handler
		handler.handleHandshakeResp = func(ctx ConnContext, resp *pnet.HandshakeResp) error {
			inUser = resp.User
			resp.User = reUser
			resp.Attrs = reAttrs
			return nil
		}
		handler.getCapability = func() pnet.Capability {
			return reCap
		}
	})
	checker := func() {
		require.Equal(t, ts.mc.username, inUser)
		require.Equal(t, reUser, ts.mb.username)
		require.Equal(t, reAttrs, ts.mb.attrs)
		require.Equal(t, reCap&pnet.ClientDeprecateEOF, pnet.Capability(ts.mb.capability)&pnet.ClientDeprecateEOF)
	}
	ts.authenticateFirstTime(t, func(t *testing.T, ts *testSuite) {})
	checker()
	ts.authenticateSecondTime(t, func(t *testing.T, ts *testSuite) {})
	checker()
	clean()
}
