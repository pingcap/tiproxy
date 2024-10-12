// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package vip

import (
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddDelIP(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("unsupported on %s", runtime.GOOS)
	}

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

	isOtherErr := func(err error) bool {
		return strings.Contains(err.Error(), "command not found") || strings.Contains(err.Error(), "not in the sudoers file")
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
		// Maybe the command is not installed.
		if err != nil && isOtherErr(err) {
			continue
		}
		if test.addErr != "" {
			require.Error(t, err, "case %d", i)
			require.Contains(t, err.Error(), test.addErr, "case %d", i)
		} else {
			require.NoError(t, err, "case %d", i)
		}

		err = operation.SendARP()
		if err == nil || !isOtherErr(err) {
			if test.sendErr != "" {
				require.Error(t, err, "case %d", i)
				require.Contains(t, err.Error(), test.sendErr, "case %d", i)
			} else {
				require.NoError(t, err, "case %d", i)
			}
		}

		if err := operation.DeleteIP(); test.delErr != "" {
			require.Error(t, err, "case %d", i)
			require.Contains(t, err.Error(), test.delErr, "case %d", i)
		} else {
			require.NoError(t, err, "case %d", i)
		}
	}
}
