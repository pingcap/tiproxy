// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeSQLSafeCatchesPanic(t *testing.T) {
	orig := normalizeSQLFn
	t.Cleanup(func() { normalizeSQLFn = orig })
	normalizeSQLFn = func(string) string { panic("boom") }

	normalized, ok := normalizeSQLSafe("select 1")
	require.False(t, ok)
	require.Equal(t, "", normalized)
}

func TestIsBeginStmtNoPanicOnNormalizePanic(t *testing.T) {
	orig := normalizeSQLFn
	t.Cleanup(func() { normalizeSQLFn = orig })
	normalizeSQLFn = func(string) string { panic("boom") }

	require.False(t, isBeginStmt("begin"))
}
