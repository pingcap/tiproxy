// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAes256(t *testing.T) {
	// prepare a file
	dir := t.TempDir()
	path := filepath.Join(dir, "test")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	require.NoError(t, err)

	// write with encryption
	key := genAesKey()
	aesWriter, err := newAESCTRWriter(file, key)
	require.NoError(t, err)
	n, err := aesWriter.Write([]byte("test"))
	require.Equal(t, 4, n)
	require.NoError(t, err)
	require.NoError(t, aesWriter.Close())
	// aesWriter has closed the file
	require.Error(t, file.Close())

	// read directly, the data is encrypted
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(data), 4)

	// read with decryption
	file, err = os.OpenFile(path, os.O_RDONLY, 0600)
	require.NoError(t, err)
	aesReader, err := newAESCTRReader(file, key)
	require.NoError(t, err)
	data = make([]byte, 100)
	n, err = io.ReadFull(aesReader, data)
	require.Equal(t, len("test"), n)
	require.ErrorContains(t, err, "EOF")
	require.Equal(t, []byte("test"), data[:n])
}

func TestEncryptOpts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test")
	keyFile := filepath.Join(dir, "key")
	require.NoError(t, os.WriteFile(keyFile, genAesKey(), 0600))

	tests := []struct {
		method  string
		keyFile string
	}{
		{EncryptPlain, ""},
		{"", ""},
		{EncryptAes, keyFile},
	}
	for i, test := range tests {
		// write
		file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		require.NoError(t, err)
		writer, err := newWriterWithEncryptOpts(file, test.method, test.keyFile)
		require.NoError(t, err, "case %d", i)
		n, err := writer.Write([]byte("test"))
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.NoError(t, writer.Close())

		// read
		file, err = os.OpenFile(path, os.O_RDONLY, 0600)
		require.NoError(t, err)
		reader, err := newReaderWithEncryptOpts(file, test.method, test.keyFile)
		require.NoError(t, err)
		data := make([]byte, 100)
		n, err = io.ReadFull(reader, data)
		require.Equal(t, 4, n)
		require.ErrorContains(t, err, "EOF")
		require.Equal(t, []byte("test"), data[:n])
	}
}

func TestAes256Error(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	require.NoError(t, err)
	defer file.Close()
	keyFile := filepath.Join(dir, "key")
	require.NoError(t, os.WriteFile(keyFile, genAesKey(), 0600))
	invalidKeyFile := filepath.Join(dir, "invalid")
	require.NoError(t, os.WriteFile(invalidKeyFile, []byte("invalid"), 0600))
	noKeyFile := filepath.Join(dir, "nonexist")

	tests := []struct {
		method  string
		keyFile string
	}{
		{"unknown", keyFile},
		{EncryptAes, ""},
		{EncryptAes, noKeyFile},
		{EncryptAes, invalidKeyFile},
	}
	for i, test := range tests {
		_, err = newWriterWithEncryptOpts(file, test.method, test.keyFile)
		require.Error(t, err, "case %d", i)
		_, err = newReaderWithEncryptOpts(file, test.method, test.keyFile)
		require.Error(t, err, "case %d", i)
	}
}

func genAesKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	return key
}
