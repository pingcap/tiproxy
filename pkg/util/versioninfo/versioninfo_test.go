// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package versioninfo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompareVersion(t *testing.T) {
	tests := []struct {
		v1 string
		v2 string
		gt bool
	}{
		{
			"v6.5.0",
			"v6.5.0",
			true,
		},
		{
			"v6.5.1",
			"v6.5.0",
			true,
		},
		{
			"v7.5.0",
			"v6.5.0",
			true,
		},
		{
			"v5.4.1",
			"v6.5.0",
			false,
		},
		{
			"nightly",
			"v6.5.0",
			true,
		},
		{
			"None",
			"v6.5.0",
			true,
		},
		{
			"v5.4.0-alpha",
			"v6.5.0",
			false,
		},
		{
			"v6.5.0-alpha",
			"v6.5.0",
			true,
		},
		{
			"v8.5.0-alpha",
			"v6.5.0",
			true,
		},
		{
			"",
			"v6.5.0",
			true,
		},
	}

	for _, test := range tests {
		require.Equal(t, test.gt, GtEqToVersion(test.v1, test.v2), "%s %s", test.v1, test.v2)
	}
}
