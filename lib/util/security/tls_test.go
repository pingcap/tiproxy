// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package security

import (
	"crypto/tls"
	"testing"

	"github.com/pingcap/tiproxy/lib/config"
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
		err    bool
	}{
		{verStr: "1.0", err: true},
		{verStr: "v1.0", err: true},
		{verStr: "TLSv1.0", err: true},
		{verStr: "1.1", err: true},
		{verStr: "v1.1", err: true},
		{verStr: "TLSv1.1", err: true},
		{verStr: "1.2", verInt: tls.VersionTLS12},
		{verStr: "v1.2", verInt: tls.VersionTLS12},
		{verStr: "TLSv1.2", verInt: tls.VersionTLS12},
		{verStr: "1.3", verInt: tls.VersionTLS13},
		{verStr: "v1.3", verInt: tls.VersionTLS13},
		{verStr: "TLSv1.3", verInt: tls.VersionTLS13},
		{verStr: "unknown", err: true},
		{verStr: "1.4", err: true},
		{verInt: tls.VersionTLS12},
	}

	for _, test := range tests {
		res, err := GetMinTLSVer(test.verStr)
		if test.err {
			require.ErrorIs(t, err, config.ErrInvalidConfigValue, test.verStr)
			continue
		}
		require.NoError(t, err, test.verStr)
		require.Equal(t, test.verInt, res, test.verStr)
	}
}

func TestRsaSize(t *testing.T) {
	sizes := []int{0, 512, 1024, 2048, 4096}
	for _, size := range sizes {
		_, _, _, err := createTempTLS(size, DefaultCertExpiration, "")
		require.NoError(t, err)
	}
	_, _, _, err := createTempTLS(config.MaxRSAKeySize+1, DefaultCertExpiration, "")
	require.Error(t, err)
}
