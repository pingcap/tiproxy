// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"compress/gzip"
	"io"

	"go.uber.org/zap"
)

var _ io.WriteCloser = (*compressWriter)(nil)

type compressWriter struct {
	io.WriteCloser
	internalWriter io.WriteCloser
	lg             *zap.Logger
}

func newCompressWriter(lg *zap.Logger, writer io.WriteCloser) *compressWriter {
	return &compressWriter{
		WriteCloser:    gzip.NewWriter(writer),
		internalWriter: writer,
		lg:             lg,
	}
}

func (w *compressWriter) Close() error {
	if err := w.WriteCloser.Close(); err != nil {
		w.lg.Warn("failed to close writer", zap.Error(err))
	}
	return w.internalWriter.Close()
}

var _ io.Reader = (*compressReader)(nil)

type compressReader struct {
	*gzip.Reader
}

func newCompressReader(reader io.Reader) (*compressReader, error) {
	gr, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	return &compressReader{
		Reader: gr,
	}, nil
}
