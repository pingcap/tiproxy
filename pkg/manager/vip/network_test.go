// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

// go:build linux

package vip

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddDelIP(t *testing.T) {
	tests := []struct {
		virtualIP string
		link      string
		initErr   string
		addErr    string
	}{
		{
			virtualIP: "127.0.0.2/24",
			link:      "lo",
		},
		{
			virtualIP: "0.0.0.0/24",
			link:      "lo",
			initErr:   "cannot assign requested address",
		},
		{
			virtualIP: "127.0.0.2/24",
			link:      "unknown",
			addErr:    "Link not found",
		},
	}

	for i, test := range tests {
		operation, err := NewNetworkOperation(test.virtualIP, test.link)
		if test.initErr != "" {
			require.Error(t, err, "case %d", i)
			require.Contains(t, err.Error(), test.initErr, "case %d", i)
			continue
		}
		require.NoError(t, err, "case %d", i)
		require.NotNil(t, operation, "case %d", i)
		if err := operation.AddIP(); test.addErr != "" {
			require.Error(t, err, "case %d", i)
			require.Contains(t, err.Error(), test.addErr, "case %d", i)
		} else {
			require.NoError(t, err, "case %d", i)
		}
		err = operation.DeleteIP()
		require.Error(t, err, "case %d", i)
	}
}
