// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var testNamespaceConfig = Namespace{
	Namespace: "test_ns",
	Frontend: FrontendNamespace{
		User: "xx",
		Security: TLSConfig{
			CA:        "t",
			Cert:      "t",
			Key:       "t",
			AutoCerts: true,
		},
	},
	Backend: BackendNamespace{
		Instances: []string{"127.0.0.1:4000", "127.0.0.1:4001"},
		Security: TLSConfig{
			CA:     "t",
			Cert:   "t",
			Key:    "t",
			SkipCA: true,
		},
	},
}

func TestNamespaceConfig(t *testing.T) {
	data1, err := testNamespaceConfig.ToBytes()
	require.NoError(t, err)
	cfg, err := NewNamespace(data1)
	require.NoError(t, err)
	data2, err := cfg.ToBytes()
	require.NoError(t, err)
	require.Equal(t, data1, data2)
}
