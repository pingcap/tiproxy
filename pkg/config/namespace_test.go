package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var testNamespaceConfig = Namespace{
	Namespace: "test_ns",
	Frontend:  FrontendNamespace{},
	Backend: BackendNamespace{
		Instances:    []string{"127.0.0.1:4000", "127.0.0.1:4001"},
		SelectorType: "random",
	},
}

func TestNamespaceConfig(t *testing.T) {
	data, err := testNamespaceConfig.ToBytes()
	require.NoError(t, err)
	cfg, err := NewNamespaceConfig(data)
	require.NoError(t, err)
	require.Equal(t, testNamespaceConfig, *cfg)
}
