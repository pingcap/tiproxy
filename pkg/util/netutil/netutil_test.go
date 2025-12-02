// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package netutil

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCIDR(t *testing.T) {
	tests := []struct {
		cidrs   []string
		success bool
	}{
		{
			cidrs:   []string{"1.1.1.1"},
			success: true,
		},
		{
			cidrs:   []string{"1.1.1.1/32"},
			success: true,
		},
		{
			cidrs:   []string{"1.1.1.1/33"},
			success: false,
		},
		{
			cidrs:   []string{"1.1.1.1/31"},
			success: true,
		},
		{
			cidrs:   []string{"1.1.1.1/30", "abc"},
			success: false,
		},
	}

	for _, test := range tests {
		list, err := ParseCIDRList(test.cidrs)
		if test.success {
			require.NoError(t, err)
			require.Equal(t, len(test.cidrs), len(list))
		} else {
			require.Error(t, err)
		}
	}
}

func TestContainIP(t *testing.T) {
	tests := []struct {
		ip      string
		cidrs   []string
		success bool
	}{
		{
			ip:      "1.1.1.1",
			cidrs:   []string{"1.1.1.1/32"},
			success: true,
		},
		{
			ip:      "1.1.1.1",
			cidrs:   []string{"1.1.1.1"},
			success: true,
		},
		{
			ip:      "1.1.1.2",
			cidrs:   []string{"1.1.1.1"},
			success: false,
		},
		{
			ip:      "1.1.1.2",
			cidrs:   []string{"1.1.1.1/30"},
			success: true,
		},
		{
			ip:      "1.1.1.100",
			cidrs:   []string{"1.1.1.1/30"},
			success: false,
		},
		{
			ip:      "1.1.1.100",
			cidrs:   []string{"1.1.1.1/30", "1.1.1.101/30"},
			success: true,
		},
	}

	for _, test := range tests {
		list, err := ParseCIDRList(test.cidrs)
		require.NoError(t, err, "ip: %s, cidrs: %v", test.ip, test.cidrs)
		contain, _ := CIDRContainsIP(list, net.ParseIP(test.ip))
		require.Equal(t, test.success, contain, "ip: %s, cidrs: %v", test.ip, test.cidrs)
	}
}

func TestIsPrivate(t *testing.T) {
	tests := []struct {
		ip      string
		private bool
	}{
		{
			ip:      "8.8.8.8",
			private: false,
		},
		{
			ip:      "192.168.1.1",
			private: true,
		},
		{
			ip:      "172.16.0.1",
			private: true,
		},
		{
			ip:      "10.0.0.1",
			private: true,
		},
		{
			ip:      "127.0.0.1",
			private: true,
		},
		{
			ip:      "::1",
			private: true,
		},
		{
			ip:      "169.254.1.1",
			private: true,
		},
		{
			ip:      "100.64.1.1",
			private: true,
		},
		{
			ip:      "2001:4860:4860::8888",
			private: false,
		},
		{
			ip:      "fc00::1",
			private: true,
		},
	}

	for _, test := range tests {
		ip := net.ParseIP(test.ip)
		require.Equal(t, test.private, IsPrivate(ip), "ip: %s", test.ip)
	}
}
