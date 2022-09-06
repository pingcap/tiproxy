package config

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

var testNamespaceConfig = Namespace{
	Namespace: "test_ns",
	Frontend: FrontendNamespace{
		Security: TLSCert{
			CA:   "t",
			Cert: "t",
			Key:  "t",
		},
	},
	Backend: BackendNamespace{
		Instances:    []string{"127.0.0.1:4000", "127.0.0.1:4001"},
		SelectorType: "random",
		Security: TLSCert{
			CA:   "t",
			Cert: "t",
			Key:  "t",
		},
	},
}

func TestNamespaceConfig(t *testing.T) {
	data, err := yaml.Marshal(testNamespaceConfig)
	require.NoError(t, err)
	var cfg Namespace
	require.NoError(t, yaml.Unmarshal(data, &cfg))
	require.Equal(t, testNamespaceConfig, cfg)
}
