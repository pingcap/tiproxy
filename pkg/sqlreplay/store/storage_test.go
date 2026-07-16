// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	// write a file
	dir := t.TempDir()
	st, err := NewStorage(dir)
	require.NoError(t, err)
	fileWriter, err := st.Create(context.Background(), "test_file", &storeapi.WriterOption{})
	require.NoError(t, err)
	sWriter := NewStorageWriter(fileWriter)
	n, err := sWriter.Write([]byte("test"))
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.NoError(t, sWriter.Close())

	// read the file
	fileReader, err := st.Open(context.Background(), "test_file", &storeapi.ReaderOption{})
	require.NoError(t, err)
	data := make([]byte, 100)
	n, err = io.ReadFull(fileReader, data)
	require.ErrorContains(t, err, "EOF")
	require.Equal(t, 4, n)
	require.NoError(t, fileReader.Close())
	st.Close()

	// write fails if the directory is a file
	path := filepath.Join(dir, "test_file")
	st, err = NewStorage(path)
	require.NoError(t, err)
	fileWriter, err = st.Create(context.Background(), "test", &storeapi.WriterOption{})
	require.Error(t, err)
	require.Nil(t, fileWriter)
	st.Close()

	// if the file doesn't exist, create a one
	path = filepath.Join(dir, "test_dir")
	st, err = NewStorage(path)
	require.NoError(t, err)
	fileWriter, err = st.Create(context.Background(), "test", &storeapi.WriterOption{})
	require.NoError(t, err)
	require.NoError(t, fileWriter.Close(context.Background()))

	// read fails if the file doesn't exist
	fileReader, err = st.Open(context.Background(), "not_exist", &storeapi.ReaderOption{})
	require.Error(t, err)
	require.Nil(t, fileReader)
	st.Close()
}
