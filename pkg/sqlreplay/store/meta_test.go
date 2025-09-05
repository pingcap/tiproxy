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
	storage, err := NewStorage(dir)
	require.NoError(t, err)
	defer storage.Close()
	for range 2 {
		m1 := Meta{
			Duration: 10 * time.Second,
			Cmds:     100,
		}
		require.NoError(t, m1.Write(storage))
		m2 := Meta{}
		require.NoError(t, m2.Read(storage))
		require.Equal(t, m1, m2)
	}

	m3 := Meta{}
	require.NoError(t, os.Remove(filepath.Join(dir, metaFile)))
	require.Error(t, m3.Read(storage))
}

func TestPrecheckMeta(t *testing.T) {
	dir := t.TempDir()
	storage, err := NewStorage(dir)
	require.NoError(t, err)
	defer storage.Close()
	require.NoError(t, PreCheckMeta(storage))
	f, err := os.Create(filepath.Join(dir, metaFile))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Error(t, PreCheckMeta(storage))
}
