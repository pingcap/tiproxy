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

func TestAes256(t *testing.T) {
	dir := t.TempDir()
	rotateWriter := newRotateWriter(WriterCfg{
		Dir:      dir,
		FileSize: 1,
	})
	keyFile := filepath.Join(dir, "key")
	genAesKey(t, keyFile)
	aesWriter, err := newAESCTRWriter(rotateWriter, keyFile)
	require.NoError(t, err)
	require.NoError(t, aesWriter.Write([]byte("test")))
	require.NoError(t, aesWriter.Close())

	rotateReader := newRotateReader(zap.NewNop(), dir)
	aesReader, err := newAESCTRReader(rotateReader, keyFile)
	require.NoError(t, err)
	data := make([]byte, 100)
	n, err := io.ReadFull(aesReader, data)
	require.Equal(t, len("test"), n)
	require.ErrorContains(t, err, "unexpected EOF")
	require.Equal(t, []byte("test"), data[:n])
	require.Equal(t, fileName, aesReader.CurFile())
	aesReader.Close()
}

func TestAes256Error(t *testing.T) {
	dir := t.TempDir()
	rotateWriter := newRotateWriter(WriterCfg{
		Dir:      dir,
		FileSize: 1,
	})
	keyFile := filepath.Join(dir, "key")
	_, err := newAESCTRWriter(rotateWriter, keyFile)
	require.Error(t, err)

	rotateReader := newRotateReader(zap.NewNop(), dir)
	_, err = newAESCTRReader(rotateReader, keyFile)
	require.Error(t, err)
}

func genAesKey(t *testing.T, keyFile string) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	require.NoError(t, os.WriteFile(keyFile, key, 0600))
}
