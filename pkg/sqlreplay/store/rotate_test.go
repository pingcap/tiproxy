// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"compress/gzip"
	"errors"
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
	writer := newRotateWriter(zap.NewNop(), WriterCfg{
		Dir:      tmpDir,
		FileSize: 1000,
	})
	data := make([]byte, 100)
	for i := 0; i < 25; i++ {
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
	for i, test := range tests {
		writer := newRotateWriter(zap.NewNop(), WriterCfg{
			Dir:      tmpDir,
			Compress: test.compress,
		})
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

func TestParseFileTs(t *testing.T) {
	tests := []struct {
		fileName string
		ts       int64
	}{
		{"traffic-2024-08-29T17-37-12.477.log", 1724953032477},
		{"traffic-2024-08-29T17-37-12.477.log.gz", 1724953032477},
		{"traffic-2024-08-29T17-37-12.477.gz", 0},
		{"traffic-2024-08-29T17-37-12.log", 0},
		{"traffic-2024-08-29T17-37-12.log.gz", 0},
		{"traffic-.log", 0},
		{"traffic-.log.gz", 0},
		{"test.log", 0},
		{"test", 0},
		{"traffic.log.gz", 0},
	}

	for i, test := range tests {
		ts := parseFileTs(test.fileName)
		require.Equal(t, test.ts, ts, "case %d", i)
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
				"traffic.log",
			},
			order: []string{
				"traffic.log",
			},
		},
		{
			fileNames: []string{
				"traffic-2024-08-29T17-37-12.477.log.gz",
				"traffic.log",
			},
			order: []string{
				"traffic-2024-08-29T17-37-12.477.log.gz",
				"traffic.log",
			},
		},
		{
			fileNames: []string{
				"traffic-2024-08-29T17-37-12.477.log.gz",
				"traffic-2024-08-30T17-37-12.477.log.gz",
				"traffic.log",
			},
			order: []string{
				"traffic-2024-08-29T17-37-12.477.log.gz",
				"traffic-2024-08-30T17-37-12.477.log.gz",
				"traffic.log",
			},
		},
		{
			fileNames: []string{
				"traffic-2024-08-29T17-37-12.477.log",
				"traffic-2024-08-30T17-37-12.477.log",
				"traffic.log",
			},
			order: []string{
				"traffic-2024-08-29T17-37-12.477.log",
				"traffic-2024-08-30T17-37-12.477.log",
				"traffic.log",
			},
		},
		{
			fileNames: []string{
				"traffic-2024-08-29T17-37-12.477.log.gz",
				"traffic-2024-08-30T17-37-12.477.log.gz",
				"traffic.log",
				"meta",
				"dir",
			},
			order: []string{
				"traffic-2024-08-29T17-37-12.477.log.gz",
				"traffic-2024-08-30T17-37-12.477.log.gz",
				"traffic.log",
			},
		},
	}

	dir := t.TempDir()
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
		l := newRotateReader(lg, dir)
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
	for _, compress := range []bool{true, false} {
		require.NoError(t, os.RemoveAll(tmpDir))
		require.NoError(t, os.MkdirAll(tmpDir, 0777))

		writer := newRotateWriter(zap.NewNop(), WriterCfg{
			Dir:      tmpDir,
			FileSize: 1000,
			Compress: compress,
		})
		data := make([]byte, 100)
		for i := 0; i < 12; i++ {
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
		l := newRotateReader(lg, tmpDir)
		for i := 0; i < 12; i++ {
			data = make([]byte, 100)
			_, err := io.ReadFull(l, data)
			require.NoError(t, err)
			for j := 0; j < 100; j++ {
				require.Equal(t, byte(0), data[j])
			}
		}
		data = make([]byte, 1)
		_, err := l.Read(data)
		require.True(t, errors.Is(err, io.EOF))
		l.Close()
	}
}
