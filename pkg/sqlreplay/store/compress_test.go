// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCompressReadWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test")

	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	require.NoError(t, err)
	writer := newCompressWriter(zap.NewNop(), file)
	n, err := writer.Write([]byte("test"))
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.NoError(t, writer.Close())
	// file is already closed
	require.Error(t, file.Close())

	file, err = os.OpenFile(path, os.O_RDONLY, 0600)
	require.NoError(t, err)
	reader, err := newCompressReader(file)
	require.NoError(t, err)
	data := make([]byte, 100)
	n, err = io.ReadFull(reader, data)
	require.Equal(t, 4, n)
	require.ErrorContains(t, err, "EOF")
}
