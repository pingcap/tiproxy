package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var testNamespaceConfig = Namespace{
	Version:   "v1",
	Namespace: "test_ns",
	Frontend: FrontendNamespace{
		AllowedDBs:   []string{"db0", "db1"},
		SlowSQLTime:  10,
		DeniedIPs:    []string{"127.0.0.0", "128.0.0.0"},
		Usernames:    []string{"user0", "user1"},
		SQLWhiteList: []SQLInfo{},
		SQLBlackList: []SQLInfo{},
	},
	Backend: BackendNamespace{
		Instances:    []string{"127.0.0.1:4000", "127.0.0.1:4001"},
		SelectorType: "random",
	},
	Breaker: BreakerInfo{
		Strategies: []StrategyInfo{},
	},
}

func TestNamespaceConfig(t *testing.T) {
	data, err := testNamespaceConfig.ToBytes()
	require.NoError(t, err)
	cfg, err := NewNamespaceConfig(data)
	require.NoError(t, err)
	require.Equal(t, testNamespaceConfig, *cfg)
}
