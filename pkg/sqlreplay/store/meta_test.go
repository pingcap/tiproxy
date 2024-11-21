// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMeta(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 2; i++ {
		m1 := Meta{
			Duration: 10 * time.Second,
			Cmds:     100,
		}
		require.NoError(t, m1.Write(dir))
		m2 := Meta{}
		require.NoError(t, m2.Read(dir))
		require.Equal(t, m1, m2)
	}

	m3 := Meta{}
	require.NoError(t, os.Remove(filepath.Join(dir, metaFile)))
	require.Error(t, m3.Read(dir))
}

func TestPrecheckMeta(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, PreCheckMeta(dir))
	f, err := os.Create(filepath.Join(dir, metaFile))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Error(t, PreCheckMeta(dir))
}
