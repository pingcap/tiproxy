// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package security

import (
	"crypto/tls"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/stretchr/testify/require"
)

func BenchmarkCreateTLS(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _, _, err := createTempTLS(0, DefaultCertExpiration, "")
		require.Nil(b, err)
	}
}

func TestGetMinTLSVer(t *testing.T) {
	tests := []struct {
		verStr string
		verInt uint16
		warn   string
	}{
		{
			verStr: "v1.0",
			verInt: tls.VersionTLS10,
			warn:   "not recommended",
		},
		{
			verStr: "v1.1",
			verInt: tls.VersionTLS11,
			warn:   "not recommended",
		},
		{
			verStr: "v1.2",
			verInt: tls.VersionTLS12,
		},
		{
			verStr: "v1.3",
			verInt: tls.VersionTLS13,
		},
		{
			verStr: "unknown",
			verInt: tls.VersionTLS12,
			warn:   "Invalid TLS version",
		},
		{
			verInt: tls.VersionTLS12,
		},
	}

	for _, test := range tests {
		lg, text := logger.CreateLoggerForTest(t)
		res := GetMinTLSVer(test.verStr, lg)
		require.Equal(t, test.verInt, res)
		if len(test.warn) > 0 {
			require.Contains(t, text.String(), test.warn)
		} else {
			require.Empty(t, text.String())
		}
	}
}

func TestRsaSize(t *testing.T) {
	sizes := []int{0, 512, 1024, 2048, 4096}
	for _, size := range sizes {
		_, _, _, err := createTempTLS(size, DefaultCertExpiration, "")
		require.Nil(t, err)
	}
}
