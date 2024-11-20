// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestReadLine(t *testing.T) {
	tests := []struct {
		data  []string
		lines [][]string
	}{
		{
			data:  []string{},
			lines: [][]string{},
		},
		{
			data: []string{
				"hello",
			},
			lines: [][]string{
				{
					"hello",
				},
			},
		},
		{
			data: []string{
				"hello\nworld",
			},
			lines: [][]string{
				{
					"hello",
					"world",
				},
			},
		},
		{
			data: []string{
				"hello\n",
			},
			lines: [][]string{
				{
					"hello",
				},
			},
		},
		{
			data: []string{
				"hello\n",
				"world\n",
			},
			lines: [][]string{
				{
					"hello",
				},
				{
					"world",
				},
			},
		},
		{
			data: []string{
				"hello\nworld\n",
				"hello\nworld\n",
			},
			lines: [][]string{
				{
					"hello",
					"world",
				},
				{
					"hello",
					"world",
				},
			},
		},
	}

	dir := t.TempDir()
	lg, _ := logger.CreateLoggerForTest(t)
	cfg := ReaderCfg{Dir: dir}
	now := time.Now()
	for i, test := range tests {
		require.NoError(t, os.RemoveAll(dir), "case %d", i)
		require.NoError(t, os.MkdirAll(dir, 0777), "case %d", i)
		fileNames := make([]string, 0, len(test.data))
		for j, data := range test.data {
			name := fmt.Sprintf("traffic-%s.log", now.Add(time.Duration(j)*time.Second).Format(fileTsLayout))
			err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0777)
			require.NoError(t, err, "case %d", i)
			fileNames = append(fileNames, name)
		}

		l, err := NewReader(lg, cfg)
		require.NoError(t, err)
		for fileIdx := 0; fileIdx < len(test.lines); fileIdx++ {
			for lineIdx := 0; lineIdx < len(test.lines[fileIdx]); lineIdx++ {
				data, filename, idx, err := l.ReadLine()
				require.NoError(t, err)
				require.Equal(t, test.lines[fileIdx][lineIdx], string(data), "case %d file %d line %d", i, fileIdx, lineIdx)
				require.Equal(t, fileNames[fileIdx], filename, "case %d file %d", i, fileIdx)
				require.Equal(t, lineIdx+1, idx, "case %d file %d", i, fileIdx)
			}
		}
		_, _, _, err = l.ReadLine()
		require.True(t, errors.Is(err, io.EOF))
		l.Close()
	}
}

func TestRead(t *testing.T) {
	tests := []struct {
		data    []string
		str     []string
		fileIdx []int
		lineIdx []int
	}{
		{
			data:    []string{},
			str:     []string{},
			fileIdx: []int{},
			lineIdx: []int{},
		},
		{
			data: []string{
				"hello",
			},
			str: []string{
				"hello",
			},
			fileIdx: []int{0},
			lineIdx: []int{1},
		},
		{
			data: []string{
				"hel\nlo",
			},
			str: []string{
				"hel\nl",
			},
			fileIdx: []int{0},
			lineIdx: []int{1},
		},
		{
			data: []string{
				"中hello",
			},
			str: []string{
				"中he",
			},
			fileIdx: []int{0},
			lineIdx: []int{1},
		},
		{
			data: []string{
				"hello\nworld\nhello",
			},
			str: []string{
				"hello",
				"\nworl",
				"d\nhel",
			},
			fileIdx: []int{0, 0, 0},
			lineIdx: []int{1, 1, 2},
		},
		{
			data: []string{
				"hello",
				"world",
			},
			str: []string{
				"hello",
				"world",
			},
			fileIdx: []int{0, 1},
			lineIdx: []int{1, 1},
		},
	}

	dir := t.TempDir()
	lg, _ := logger.CreateLoggerForTest(t)
	cfg := ReaderCfg{Dir: dir}
	now := time.Now()
	for i, test := range tests {
		require.NoError(t, os.RemoveAll(dir), "case %d", i)
		require.NoError(t, os.MkdirAll(dir, 0777), "case %d", i)
		fileNames := make([]string, 0, len(test.data))
		for j, data := range test.data {
			name := fmt.Sprintf("traffic-%s.log", now.Add(time.Duration(j)*time.Second).Format(fileTsLayout))
			err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0777)
			require.NoError(t, err, "case %d", i)
			fileNames = append(fileNames, name)
		}

		l, err := NewReader(lg, cfg)
		require.NoError(t, err)
		data := make([]byte, 5)
		for j := 0; j < len(test.str); j++ {
			filename, idx, err := l.Read(data)
			require.NoError(t, err)
			require.Equal(t, test.str[j], string(data), "case %d", i)
			require.Equal(t, fileNames[test.fileIdx[j]], filename, "case %d", i)
			require.Equal(t, test.lineIdx[j], idx, "case %d", i)
		}
		_, _, err = l.Read(data)
		require.True(t, errors.Is(err, io.EOF), "case %d, err %v", i, err)
		l.Close()
	}
}

func TestEncryptMethods(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "key")
	genAesKey(t, keyFile)
	for _, method := range []string{
		"",
		EncryptPlain,
		EncryptAes,
	} {
		writer, err := NewWriter(WriterCfg{
			Dir:           dir,
			EncryptMethod: method,
			KeyFile:       keyFile,
		})
		require.NoError(t, err, method)
		require.NoError(t, writer.Write([]byte("test")), method)
		require.NoError(t, writer.Close(), method)

		reader, err := NewReader(zap.NewNop(), ReaderCfg{
			Dir:           dir,
			EncryptMethod: method,
			KeyFile:       keyFile,
		})
		require.NoError(t, err, method)
		data := make([]byte, 4)
		curFile, lineIdx, err := reader.Read(data)
		require.NoError(t, err, method)
		require.Equal(t, fileName, curFile, method)
		require.Equal(t, 1, lineIdx, method)
		reader.Close()
	}
}
