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
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/stretchr/testify/require"
)

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
	cfg := LoaderCfg{Dir: dir}
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
		l := NewLoader(lg, cfg)
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
	cfg := LoaderCfg{Dir: dir}
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

		l := NewLoader(lg, cfg)
		for fileIdx := 0; fileIdx < len(test.lines); fileIdx++ {
			for lineIdx := 0; lineIdx < len(test.lines[fileIdx]); lineIdx++ {
				data, filename, idx, err := l.ReadLine()
				require.NoError(t, err)
				require.Equal(t, test.lines[fileIdx][lineIdx], string(data), "case %d file %d line %d", i, fileIdx, lineIdx)
				require.Equal(t, fileNames[fileIdx], filename, "case %d file %d", i, fileIdx)
				require.Equal(t, lineIdx+1, idx, "case %d file %d", i, fileIdx)
			}
		}
		_, _, _, err := l.ReadLine()
		require.True(t, errors.Is(err, io.EOF))
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
	cfg := LoaderCfg{Dir: dir}
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

		l := NewLoader(lg, cfg)
		for j := 0; j < len(test.str); j++ {
			data, filename, idx, err := l.Read(5)
			require.NoError(t, err)
			require.Equal(t, test.str[j], string(data), "case %d", i)
			require.Equal(t, fileNames[test.fileIdx[j]], filename, "case %d", i)
			require.Equal(t, test.lineIdx[j], idx, "case %d", i)
		}
		_, _, _, err := l.Read(5)
		require.True(t, errors.Is(err, io.EOF))
	}
}
