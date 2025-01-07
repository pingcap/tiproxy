// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"io"
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	dir := t.TempDir()
	st, err := NewStorage(dir)
	require.NoError(t, err)
	fileWriter, err := st.Create(context.Background(), "test", &storage.WriterOption{})
	require.NoError(t, err)
	sWriter := NewStorageWriter(fileWriter)
	n, err := sWriter.Write([]byte("test"))
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.NoError(t, sWriter.Close())

	fileReader, err := st.Open(context.Background(), "test", &storage.ReaderOption{})
	require.NoError(t, err)
	data := make([]byte, 100)
	n, err = io.ReadFull(fileReader, data)
	require.ErrorContains(t, err, "EOF")
	require.Equal(t, 4, n)
	require.NoError(t, fileReader.Close())
}
