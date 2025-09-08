// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFileRotation(t *testing.T) {
	tmpDir := t.TempDir()
	storage, err := NewStorage(tmpDir)
	require.NoError(t, err)
	defer storage.Close()
	writer, err := newRotateWriter(zap.NewNop(), storage, WriterCfg{
		Dir:      tmpDir,
		FileSize: 1000,
	})
	require.NoError(t, err)
	data := make([]byte, 100)
	for range 25 {
		n, err := writer.Write(data)
		require.NoError(t, err)
		require.Equal(t, len(data), n)
	}
	require.Equal(t, 3, countTrafficFiles(t, tmpDir))
	require.NoError(t, writer.Close())
}

func listFiles(t *testing.T, dir string) []string {
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(files))
	for _, f := range files {
		names = append(names, f.Name())
	}
	return names
}

func countTrafficFiles(t *testing.T, dir string) int {
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	count := 0
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "traffic") {
			count++
		}
	}
	return count
}

func TestCompress(t *testing.T) {
	tests := []struct {
		compress bool
		ext      string
	}{
		{
			compress: true,
			ext:      fileCompressFormat,
		},
		{
			compress: false,
			ext:      fileNameSuffix,
		},
	}

	tmpDir := t.TempDir()
	storage, err := NewStorage(tmpDir)
	require.NoError(t, err)
	defer storage.Close()
	for i, test := range tests {
		writer, err := newRotateWriter(zap.NewNop(), storage, WriterCfg{
			Dir:      tmpDir,
			Compress: test.compress,
		})
		require.NoError(t, err, "case %d", i)
		n, err := writer.Write([]byte("test"))
		require.NoError(t, err, "case %d", i)
		require.Equal(t, 4, n, "case %d", i)
		files := listFiles(t, tmpDir)
		require.Len(t, files, 1, "case %d", i)
		require.True(t, strings.HasSuffix(files[0], test.ext), "case %d", i)
		require.NoError(t, os.Remove(filepath.Join(tmpDir, files[0])), "case %d", i)
		require.NoError(t, writer.Close(), "case %d", i)
	}
}

func TestParseFileIdx(t *testing.T) {
	tests := []struct {
		fileName string
		fileIdx  int
	}{
		{"traffic-1.log", 1},
		{"traffic-2.log.gz", 2},
		{"traffic-100.log.gz", 100},
		{"traffic-2024-08-29T17-37-12.log", 0},
		{"traffic-2024-08-29T17-37-12.log.gz", 0},
		{"traffic-.log", 0},
		{"traffic-.log.gz", 0},
		{"traffic.log", 0},
		{"traffic-100.gz", 0},
		{"test", 0},
		{"traffic.log.gz", 0},
	}

	for i, test := range tests {
		idx := parseFileIdx(test.fileName)
		require.Equal(t, test.fileIdx, idx, "case %d", i)
	}
}

func TestIterateFiles(t *testing.T) {
	tests := []struct {
		fileNames []string
		order     []string
	}{
		{
			fileNames: []string{},
			order:     []string{},
		},
		{
			fileNames: []string{
				"traffic-1.log.gz",
			},
			order: []string{
				"traffic-1.log.gz",
			},
		},
		{
			fileNames: []string{
				"traffic-2.log.gz",
				"traffic-1.log.gz",
			},
			order: []string{
				"traffic-1.log.gz",
				"traffic-2.log.gz",
			},
		},
		{
			fileNames: []string{
				"traffic-1.log",
				"traffic-2.log",
			},
			order: []string{
				"traffic-1.log",
				"traffic-2.log",
			},
		},
		{
			fileNames: []string{
				"traffic-1.log.gz",
				"traffic-2.log.gz",
				"traffic.log",
				"meta",
				"dir",
			},
			order: []string{
				"traffic-1.log.gz",
				"traffic-2.log.gz",
			},
		},
	}

	dir := t.TempDir()
	storage, err := NewStorage(dir)
	require.NoError(t, err)
	defer storage.Close()
	lg, _ := logger.CreateLoggerForTest(t)
	for i, test := range tests {
		require.NoError(t, os.RemoveAll(dir), "case %d", i)
		require.NoError(t, os.MkdirAll(dir, 0777), "case %d", i)
		for _, name := range test.fileNames {
			if name == "dir" {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, name), 0777), "case %d", i)
				break
			}
			f, err := os.Create(filepath.Join(dir, name))
			require.NoError(t, err, "case %d", i)
			if strings.HasSuffix(name, ".gz") {
				w := gzip.NewWriter(f)
				_, err := w.Write([]byte{})
				require.NoError(t, err)
				require.NoError(t, w.Close())
			}
			require.NoError(t, f.Close())
		}
		l, err := newRotateReader(lg, storage, ReaderCfg{Dir: dir})
		require.NoError(t, err)
		fileOrder := make([]string, 0, len(test.order))
		for {
			if err := l.nextReader(); err != nil {
				require.True(t, errors.Is(err, io.EOF))
				break
			}
			fileOrder = append(fileOrder, l.curFileName)
		}
		require.Equal(t, test.order, fileOrder)
	}
}

func TestReadGZip(t *testing.T) {
	tmpDir := t.TempDir()
	storage, err := NewStorage(tmpDir)
	require.NoError(t, err)
	defer storage.Close()
	for _, compress := range []bool{true, false} {
		require.NoError(t, os.RemoveAll(tmpDir))
		require.NoError(t, os.MkdirAll(tmpDir, 0777))

		writer, err := newRotateWriter(zap.NewNop(), storage, WriterCfg{
			Dir:      tmpDir,
			FileSize: 1000,
			Compress: compress,
		})
		require.NoError(t, err)
		data := make([]byte, 100)
		for range 12 {
			n, err := writer.Write(data)
			require.NoError(t, err)
			require.Equal(t, len(data), n)
		}
		files := listFiles(t, tmpDir)
		for _, f := range files {
			require.True(t, strings.HasPrefix(f, fileNamePrefix))
			if compress {
				require.True(t, strings.HasSuffix(f, fileCompressFormat))
			} else {
				require.True(t, strings.HasSuffix(f, fileNameSuffix))
			}
		}
		require.NoError(t, writer.Close())

		lg, _ := logger.CreateLoggerForTest(t)
		l, err := newRotateReader(lg, storage, ReaderCfg{Dir: tmpDir})
		require.NoError(t, err)
		for range 12 {
			data = make([]byte, 100)
			_, err := io.ReadFull(l, data)
			require.NoError(t, err)
			for j := range 100 {
				require.Equal(t, byte(0), data[j])
			}
		}
		data = make([]byte, 1)
		_, err = l.Read(data)
		require.True(t, errors.Is(err, io.EOF))
		l.Close()
	}
}

func TestCompressAndEncrypt(t *testing.T) {
	tmpDir := t.TempDir()
	storage, err := NewStorage(tmpDir)
	require.NoError(t, err)
	defer storage.Close()
	key := genAesKey()

	// write with compression and encryption
	writer, err := newRotateWriter(zap.NewNop(), storage, WriterCfg{
		Dir:              tmpDir,
		FileSize:         1,
		Compress:         true,
		EncryptionMethod: EncryptAes,
		EncryptionKey:    key,
	})
	require.NoError(t, err)
	// write into 2 files
	for range 2 {
		_, err = writer.Write([]byte("test"))
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())

	// make sure data is compressed after encryption
	for i := range 2 {
		file, err := os.Open(filepath.Join(tmpDir, fmt.Sprintf("traffic-%d.log.gz", i+1)))
		require.NoError(t, err)
		greader, err := gzip.NewReader(file)
		require.NoError(t, err)
		data := make([]byte, 1000)
		n, err := io.ReadFull(greader, data)
		require.ErrorContains(t, err, "EOF")
		require.Equal(t, 20, n)
		require.NoError(t, file.Close())
	}

	// rotateReader is able to read the file
	reader, err := newRotateReader(zap.NewNop(), storage, ReaderCfg{
		Dir:              tmpDir,
		EncryptionMethod: EncryptAes,
		EncryptionKey:    key,
	})
	require.NoError(t, err)
	data := make([]byte, 1000)
	n, err := io.ReadFull(reader, data)
	require.ErrorContains(t, err, "EOF")
	require.Equal(t, 8, n)
	require.Equal(t, []byte("testtest"), data[:8])
	require.NoError(t, reader.Close())
}
