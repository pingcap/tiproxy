// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package security

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkCreateTLS(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _, _, err := createTempTLS(0, DefaultCertExpiration)
		require.Nil(b, err)
	}
}
