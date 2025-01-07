// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"io"

	"github.com/pingcap/tidb/br/pkg/storage"
)

var _ io.WriteCloser = (*StorageWriter)(nil)

type StorageWriter struct {
	writer storage.ExternalFileWriter
}

func (s *StorageWriter) Write(p []byte) (n int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	return s.writer.Write(ctx, p)
}

func (s *StorageWriter) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	return s.writer.Close(ctx)
}

func NewStorageWriter(writer storage.ExternalFileWriter) *StorageWriter {
	return &StorageWriter{writer: writer}
}

func NewStorage(path string) (storage.ExternalStorage, error) {
	backend, err := storage.ParseBackend(path, &storage.BackendOptions{})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	return storage.New(ctx, backend, &storage.ExternalStorageOptions{})
}
