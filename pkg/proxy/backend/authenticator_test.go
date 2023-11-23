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
			if ts.mc.clientConfig.capability&requiredFrontendCaps != requiredFrontendCaps {
				require.ErrorIs(t, ts.mp.err, ErrClientCap)
			} else if ts.mb.backendConfig.capability&defRequiredBackendCaps != defRequiredBackendCaps {
				require.ErrorIs(t, ts.mp.err, ErrBackendCap)
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
		{
			func(cfg *testConfig) {
				cfg.backendConfig.capability &= ^pnet.ClientCompress
				cfg.backendConfig.capability &= ^pnet.ClientZstdCompressionAlgorithm
			},
			func(cfg *testConfig) {
				cfg.backendConfig.capability |= pnet.ClientCompress
				cfg.backendConfig.capability |= pnet.ClientZstdCompressionAlgorithm
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability &= ^pnet.ClientCompress
				cfg.clientConfig.capability &= ^pnet.ClientZstdCompressionAlgorithm
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability |= pnet.ClientCompress
				cfg.clientConfig.capability |= pnet.ClientZstdCompressionAlgorithm
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability |= pnet.ClientCompress
				cfg.clientConfig.capability &= ^pnet.ClientZstdCompressionAlgorithm
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
		require.Equal(t, reCap&pnet.ClientDeprecateEOF, ts.mb.capability&pnet.ClientDeprecateEOF)
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

func TestRequireBackendTLS(t *testing.T) {
	tests := []struct {
		cfg cfgOverrider
		err error
	}{
		{
			cfg: func(cfg *testConfig) {
				cfg.proxyConfig.bcConfig.RequireBackendTLS = true
				cfg.proxyConfig.backendTLSConfig = nil
				cfg.backendConfig.capability |= pnet.ClientSSL
			},
			err: ErrProxyNoTLS,
		},
		{
			cfg: func(cfg *testConfig) {
				cfg.proxyConfig.bcConfig.RequireBackendTLS = true
				cfg.backendConfig.tlsConfig = nil
				cfg.backendConfig.capability &= ^pnet.ClientSSL
			},
			err: ErrBackendNoTLS,
		},
		{
			cfg: func(cfg *testConfig) {
				cfg.proxyConfig.bcConfig.RequireBackendTLS = false
				cfg.proxyConfig.backendTLSConfig = nil
				cfg.backendConfig.tlsConfig = nil
				cfg.backendConfig.capability &= ^pnet.ClientSSL
			},
		},
	}

	tc := newTCPConnSuite(t)
	for _, tt := range tests {
		ts, clean := newTestSuite(t, tc, tt.cfg)
		ts.authenticateFirstTime(t, func(t *testing.T, ts *testSuite) {
			if tt.err != nil {
				require.ErrorIs(t, ts.mp.err, tt.err)
			} else {
				require.NoError(t, ts.mp.err)
			}
		})
		clean()
	}
}

func TestProxyProtocol(t *testing.T) {
	cfgs := [][]cfgOverrider{
		{
			func(cfg *testConfig) {
				cfg.proxyConfig.bcConfig.ProxyProtocol = true
			},
			func(cfg *testConfig) {
				cfg.proxyConfig.bcConfig.ProxyProtocol = false
			},
		},
		{
			func(cfg *testConfig) {
				cfg.backendConfig.proxyProtocol = true
			},
			func(cfg *testConfig) {
				cfg.backendConfig.proxyProtocol = false
			},
		},
		{
			func(cfg *testConfig) {
				cfg.proxyConfig.bcConfig.RequireBackendTLS = true
				cfg.backendConfig.capability |= pnet.ClientSSL
			},
			func(cfg *testConfig) {
				cfg.proxyConfig.bcConfig.RequireBackendTLS = false
				cfg.backendConfig.capability &= ^pnet.ClientSSL
			},
		},
	}

	tc := newTCPConnSuite(t)
	cfgOverriders := getCfgCombinations(cfgs)
	for _, cfgs := range cfgOverriders {
		ts, clean := newTestSuite(t, tc, cfgs...)
		ts.authenticateFirstTime(t, func(t *testing.T, ts *testSuite) {
			// TiDB proxy-protocol can be set unfallbackable, but TiProxy proxy-protocol is always fallbackable.
			// So when backend enables proxy-protocol and proxy disables it, it still works well.
			if ts.mp.bcConfig.ProxyProtocol && !ts.mb.proxyProtocol {
				err := ErrToClient(ts.mp.err)
				require.Equal(t, ErrBackendPPV2, err)
			} else {
				require.NoError(t, ts.mp.err)
			}
		})
		clean()
	}
}

func TestCompressProtocol(t *testing.T) {
	cfgs := [][]cfgOverrider{
		{
			func(cfg *testConfig) {
				cfg.backendConfig.capability &= ^pnet.ClientCompress
				cfg.backendConfig.capability &= ^pnet.ClientZstdCompressionAlgorithm
			},
			func(cfg *testConfig) {
				cfg.backendConfig.capability |= pnet.ClientCompress
				cfg.backendConfig.capability |= pnet.ClientZstdCompressionAlgorithm
			},
		},
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability &= ^pnet.ClientCompress
				cfg.clientConfig.capability &= ^pnet.ClientZstdCompressionAlgorithm
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability |= pnet.ClientCompress
				cfg.clientConfig.capability |= pnet.ClientZstdCompressionAlgorithm
				cfg.clientConfig.zstdLevel = 3
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability |= pnet.ClientCompress
				cfg.clientConfig.capability |= pnet.ClientZstdCompressionAlgorithm
				cfg.clientConfig.zstdLevel = 9
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability |= pnet.ClientCompress
				cfg.clientConfig.capability &= ^pnet.ClientZstdCompressionAlgorithm
			},
		},
	}

	checker := func(t *testing.T, ts *testSuite, referCfg *testConfig) {
		// If the client enables compression, client <-> proxy enables compression.
		if referCfg.clientConfig.capability&pnet.ClientCompress > 0 {
			require.Greater(t, ts.mp.authenticator.capability&pnet.ClientCompress, pnet.Capability(0))
			require.Greater(t, ts.mc.capability&pnet.ClientCompress, pnet.Capability(0))
		} else {
			require.Equal(t, pnet.Capability(0), ts.mp.authenticator.capability&pnet.ClientCompress)
			require.Equal(t, pnet.Capability(0), ts.mc.capability&pnet.ClientCompress)
		}
		// If both the client and the backend enables compression, proxy <-> backend enables compression.
		if referCfg.clientConfig.capability&referCfg.backendConfig.capability&pnet.ClientCompress > 0 {
			require.Greater(t, ts.mb.capability&pnet.ClientCompress, pnet.Capability(0))
		} else {
			require.Equal(t, pnet.Capability(0), ts.mb.capability&pnet.ClientCompress)
		}
		// If the client enables zstd compression, client <-> proxy enables zstd compression.
		zstdCap := pnet.ClientCompress | pnet.ClientZstdCompressionAlgorithm
		if referCfg.clientConfig.capability&zstdCap == zstdCap {
			require.Greater(t, ts.mp.authenticator.capability&pnet.ClientZstdCompressionAlgorithm, pnet.Capability(0))
			require.Greater(t, ts.mc.capability&pnet.ClientZstdCompressionAlgorithm, pnet.Capability(0))
			require.Equal(t, referCfg.clientConfig.zstdLevel, ts.mp.authenticator.zstdLevel)
		} else {
			require.Equal(t, pnet.Capability(0), ts.mp.authenticator.capability&pnet.ClientZstdCompressionAlgorithm)
			require.Equal(t, pnet.Capability(0), ts.mc.capability&pnet.ClientZstdCompressionAlgorithm)
		}
		// If both the client and the backend enables zstd compression, proxy <-> backend enables zstd compression.
		if referCfg.clientConfig.capability&referCfg.backendConfig.capability&zstdCap == zstdCap {
			require.Greater(t, ts.mb.capability&pnet.ClientZstdCompressionAlgorithm, pnet.Capability(0))
			require.Equal(t, referCfg.clientConfig.zstdLevel, ts.mb.zstdLevel)
		} else {
			require.Equal(t, pnet.Capability(0), ts.mb.capability&pnet.ClientZstdCompressionAlgorithm)
		}
	}

	tc := newTCPConnSuite(t)
	cfgOverriders := getCfgCombinations(cfgs)
	for _, cfgs := range cfgOverriders {
		referCfg := newTestConfig(cfgs...)
		ts, clean := newTestSuite(t, tc, cfgs...)
		ts.authenticateFirstTime(t, func(t *testing.T, ts *testSuite) {
			checker(t, ts, referCfg)
		})
		ts.authenticateSecondTime(t, func(t *testing.T, ts *testSuite) {
			checker(t, ts, referCfg)
		})
		clean()
	}
}

// After upgrading the backend, the backend capability may change.
func TestUpgradeBackendCap(t *testing.T) {
	cfgs := [][]cfgOverrider{
		{
			func(cfg *testConfig) {
				cfg.clientConfig.capability &= ^pnet.ClientCompress
				cfg.clientConfig.capability &= ^pnet.ClientZstdCompressionAlgorithm
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability |= pnet.ClientCompress
				cfg.clientConfig.capability |= pnet.ClientZstdCompressionAlgorithm
				cfg.clientConfig.zstdLevel = 3
			},
			func(cfg *testConfig) {
				cfg.clientConfig.capability |= pnet.ClientCompress
				cfg.clientConfig.capability &= ^pnet.ClientZstdCompressionAlgorithm
			},
		},
		{
			func(cfg *testConfig) {
				cfg.backendConfig.capability &= ^pnet.ClientCompress
				cfg.backendConfig.capability &= ^pnet.ClientZstdCompressionAlgorithm
			},
		},
	}

	tc := newTCPConnSuite(t)
	cfgOverriders := getCfgCombinations(cfgs)
	for _, cfgs := range cfgOverriders {
		referCfg := newTestConfig(cfgs...)
		ts, clean := newTestSuite(t, tc, cfgs...)
		// Before upgrade, the backend doesn't support compression.
		ts.authenticateFirstTime(t, func(t *testing.T, ts *testSuite) {
			require.Equal(t, referCfg.clientConfig.capability&pnet.ClientCompress, ts.mp.authenticator.capability&pnet.ClientCompress)
			require.Equal(t, referCfg.clientConfig.capability&pnet.ClientCompress, ts.mc.capability&pnet.ClientCompress)
			require.Equal(t, pnet.Capability(0), ts.mb.capability&pnet.ClientCompress)
		})
		// After upgrade, the backend also supports compression.
		ts.mb.backendConfig.capability |= pnet.ClientCompress
		ts.mb.backendConfig.capability |= pnet.ClientZstdCompressionAlgorithm
		ts.authenticateSecondTime(t, func(t *testing.T, ts *testSuite) {
			require.Equal(t, referCfg.clientConfig.capability&pnet.ClientCompress, ts.mc.capability&pnet.ClientCompress)
			require.Equal(t, referCfg.clientConfig.capability&pnet.ClientCompress, ts.mp.authenticator.capability&pnet.ClientCompress)
			require.Equal(t, referCfg.clientConfig.capability&pnet.ClientCompress, ts.mb.capability&pnet.ClientCompress)
			require.Equal(t, referCfg.clientConfig.capability&pnet.ClientZstdCompressionAlgorithm, ts.mc.capability&pnet.ClientZstdCompressionAlgorithm)
			require.Equal(t, referCfg.clientConfig.capability&pnet.ClientZstdCompressionAlgorithm, ts.mp.authenticator.capability&pnet.ClientZstdCompressionAlgorithm)
			require.Equal(t, referCfg.clientConfig.capability&pnet.ClientZstdCompressionAlgorithm, ts.mb.capability&pnet.ClientZstdCompressionAlgorithm)
		})
		clean()
	}
}
