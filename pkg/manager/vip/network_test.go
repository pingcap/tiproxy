// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

// go:build linux

package vip

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddDelIP(t *testing.T) {
	tests := []struct {
		virtualIP string
		link      string
		initErr   string
		addErr    string
		delErr    string
		sendErr   string
	}{
		{
			virtualIP: "127.0.0.2/24",
			link:      "lo",
		},
		{
			virtualIP: "0.0.0.0/24",
			link:      "lo",
			delErr:    "cannot assign requested address",
		},
		{
			virtualIP: "127.0.0.2/24",
			link:      "unknown",
			initErr:   "Link not found",
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

		err = operation.AddIP()
		// Maybe the privilege is not granted.
		if err != nil && strings.Contains(err.Error(), "operation not permitted") {
			continue
		}
		if test.addErr != "" {
			require.Error(t, err, "case %d", i)
			require.Contains(t, err.Error(), test.addErr, "case %d", i)
		} else {
			require.NoError(t, err, "case %d", i)
		}

		err = operation.SendARP()
		if test.sendErr != "" {
			require.Error(t, err, "case %d", i)
			require.Contains(t, err.Error(), test.sendErr, "case %d", i)
		} else {
			require.NoError(t, err, "case %d", i)
		}

		if err := operation.DeleteIP(); test.delErr != "" {
			require.Error(t, err, "case %d", i)
			require.Contains(t, err.Error(), test.delErr, "case %d", i)
		} else {
			require.NoError(t, err, "case %d", i)
		}
	}
}
