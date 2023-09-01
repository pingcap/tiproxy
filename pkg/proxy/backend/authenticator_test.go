// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
)

func TestUnsupportedCapability(t *testing.T) {
	cfgs := [][]cfgOverrider{
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability &= ^pnet.ClientSSL
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability |= pnet.ClientSSL
			},
		},
		{
			func(cfg *testConfig) {
				cfg.backendConfig.capability &= ^pnet.ClientSSL
			},
			func(cfg *testConfig) {
				cfg.backendConfig.capability |= pnet.ClientSSL
			},
		},
		{
			func(cfg *testConfig) {
				cfg.backendConfig.capability &= ^pnet.ClientDeprecateEOF
			},
			func(cfg *testConfig) {
				cfg.backendConfig.capability |= pnet.ClientDeprecateEOF
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability &= ^pnet.ClientProtocol41
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability |= pnet.ClientProtocol41
			},
		},
		{
			func(cfg *testConfig) {
				cfg.backendConfig.capability &= ^pnet.ClientPSMultiResults
			},
			func(cfg *testConfig) {
				cfg.backendConfig.capability |= pnet.ClientPSMultiResults
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability &= ^pnet.ClientPSMultiResults
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability |= pnet.ClientPSMultiResults
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
				cfg.clientConfig.capability &= ^pnet.ClientConnectWithDB
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability |= pnet.ClientConnectWithDB
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability &= ^pnet.ClientConnectAttrs
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability |= pnet.ClientConnectAttrs
				cfg.clientConfig.attrs = map[string]string{"key": "value"}
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability &= ^pnet.ClientSecureConnection
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability |= pnet.ClientSecureConnection
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

func TestEnableTLS(t *testing.T) {
	tests := []struct {
		cfg     cfgOverrider
		enabled bool
	}{
		{
			cfg: func(cfg *testConfig) {
				cfg.clientConfig.capability &= ^pnet.ClientSSL
				cfg.backendConfig.capability |= pnet.ClientSSL
			},
			enabled: false,
		},
		{
			cfg: func(cfg *testConfig) {
				cfg.clientConfig.capability |= pnet.ClientSSL
				cfg.backendConfig.capability |= pnet.ClientSSL
			},
			enabled: true,
		},
		{
			// client enables TLS but backendTLSConfig is nil
			cfg: func(cfg *testConfig) {
				cfg.clientConfig.capability |= pnet.ClientSSL
				cfg.proxyConfig.backendTLSConfig = nil
				cfg.backendConfig.capability |= pnet.ClientSSL
			},
			enabled: false,
		},
	}
	tc := newTCPConnSuite(t)
	for _, test := range tests {
		ts, clean := newTestSuite(t, tc, test.cfg)
		ts.authenticateFirstTime(t, func(t *testing.T, _ *testSuite) {
			require.Equal(t, test.enabled, ts.mb.capability&pnet.ClientSSL > 0)
		})
		clean()
	}
}

// Even if auth fails, the auth data should be passed so that `using password` in the error message is correct.
func TestAuthFail(t *testing.T) {
	cfgs := []cfgOverrider{
		func(cfg *testConfig) {
			cfg.clientConfig.authData = nil
			cfg.backendConfig.authSucceed = false
		},
		func(cfg *testConfig) {
			cfg.clientConfig.authData = []byte("dummy")
			cfg.backendConfig.authSucceed = false
		},
	}

	tc := newTCPConnSuite(t)
	for _, cfg := range cfgs {
		ts, clean := newTestSuite(t, tc, cfg)
		ts.authenticateFirstTime(t, func(t *testing.T, ts *testSuite) {
			require.Equal(t, len(ts.mc.authData), len(ts.mb.authData))
		})
		clean()
	}
}
