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
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/stretchr/testify/require"
)

func TestFileRotation(t *testing.T) {
	tmpDir := t.TempDir()
	writer := newRotateWriter(WriterCfg{
		Dir:      tmpDir,
		FileSize: 1,
		Compress: true,
	})
	defer writer.Close()

	data := make([]byte, 100*1024)
	for i := 0; i < 11; i++ {
		require.NoError(t, writer.Write(data))
	}

	// files are rotated and compressed at backendground asynchronously
	require.Eventually(t, func() bool {
		files := listFiles(t, tmpDir)
		count := 0
		compressed := false
		for _, f := range files {
			if strings.HasPrefix(f, "traffic") {
				count++
			}
			if strings.HasSuffix(f, ".gz") {
				compressed = true
			}
		}
		if count == 2 && compressed {
			return true
		}
		t.Logf("traffic files: %v", files)
		return false
	}, 5*time.Second, 10*time.Millisecond)
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

		writer := newRotateWriter(WriterCfg{
			Dir:      tmpDir,
			FileSize: 1,
			Compress: compress,
		})
		data := make([]byte, 100*1024)
		for i := 0; i < 11; i++ {
			require.NoError(t, writer.Write(data))
		}
		// files are rotated and compressed at backendground asynchronously
		if compress {
			require.Eventually(t, func() bool {
				files := listFiles(t, tmpDir)
				for _, f := range files {
					if strings.HasPrefix(f, "traffic") && strings.HasSuffix(f, ".gz") {
						return true
					}
				}
				t.Logf("traffic files: %v", files)
				return false
			}, 5*time.Second, 10*time.Millisecond)
		} else {
			time.Sleep(100 * time.Millisecond)
			files := listFiles(t, tmpDir)
			for _, f := range files {
				if strings.HasPrefix(f, "traffic") {
					require.False(t, strings.HasSuffix(f, ".gz"))
				}
			}
		}
		require.NoError(t, writer.Close())

		lg, _ := logger.CreateLoggerForTest(t)
		l := newRotateReader(lg, tmpDir)
		for i := 0; i < 11; i++ {
			data = make([]byte, 100*1024)
			_, err := io.ReadFull(l, data)
			require.NoError(t, err)
			for j := 0; j < 100*1024; j++ {
				require.Equal(t, byte(0), data[j])
			}
		}
		data = make([]byte, 1)
		_, err := l.Read(data)
		require.True(t, errors.Is(err, io.EOF))
		l.Close()
	}
}
