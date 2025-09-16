// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
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
		fileIdx  int64
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
		idx := parseFileIdx(test.fileName, fileNamePrefix)
		require.Equal(t, test.fileIdx, idx, "case %d", i)
	}
}

func TestParseFileTime(t *testing.T) {
	// Calculate the current timezone shift
	_, offset := time.Now().Zone()

	tests := []struct {
		fileName string
		fileIdx  int64
	}{
		{"tidb-audit-2025-09-10T17-01-56.073.log", 1757523716073 - int64(offset*1000)},
		{"tidb-audit-2025-09-10T17-01-56.172.log.gz", 1757523716172 - int64(offset*1000)},
		{"tidb-audit-2025-09-10T17-01-56.log.gz", 1757523716000 - int64(offset*1000)},
		{"traffic-2025-09-10T17-01-56.172.log", 0},
		{"traffic-2025-09-10T17-01-56.172.log.gz", 0},
		{"tidb-audit-.log", 0},
		{"tidb-audit-.log.gz", 0},
		{"tidb-audit.log", 0},
		{"tidb-audit-100.gz", 0},
		{"test", 0},
		{"tidb-audit.log.gz", 0},
	}

	for i, test := range tests {
		idx := parseFileTimeToIdx(test.fileName, auditFileNamePrefix)
		require.Equal(t, test.fileIdx, idx, "case %d", i)
	}
}

func TestIterateFiles(t *testing.T) {
	tests := []struct {
		format    string
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
		{
			format: cmd.FormatAuditLogPlugin,
			fileNames: []string{
				"tidb-audit-2025-09-10T17-01-56.073.log",
			},
			order: []string{
				"tidb-audit-2025-09-10T17-01-56.073.log",
			},
		},
		{
			format: cmd.FormatAuditLogPlugin,
			fileNames: []string{
				"tidb-audit-2025-09-10T17-01-56.172.log",
				"tidb-audit-2025-09-10T17-01-56.073.log",
				"tidb-audit-2025-09-10T17-01-55.976.log",
			},
			order: []string{
				"tidb-audit-2025-09-10T17-01-55.976.log",
				"tidb-audit-2025-09-10T17-01-56.073.log",
				"tidb-audit-2025-09-10T17-01-56.172.log",
			},
		},
		{
			format: cmd.FormatAuditLogPlugin,
			fileNames: []string{
				"tidb-audit.log",
				"tidb-audit-2025-09-10T17-01-55.976.log",
			},
			order: []string{
				"tidb-audit-2025-09-10T17-01-55.976.log",
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
		l, err := newRotateReader(lg, storage, ReaderCfg{Dir: dir, Format: test.format})
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

func TestFilterFileNameByStartTime(t *testing.T) {
	commandStartTime, err := time.ParseInLocation(logTimeLayout, "2025-09-10T17-01-56.050", time.Local)
	require.NoError(t, err)
	tests := []struct {
		fileName        string
		expectToInclude bool
	}{
		// Files after start time should be included
		{
			fileName:        "tidb-audit-2025-09-10T17-01-56.073.log",
			expectToInclude: true,
		},
		{
			fileName:        "tidb-audit-2025-09-10T17-01-56.172.log.gz",
			expectToInclude: true,
		},
		{
			fileName:        "tidb-audit-2025-09-11T10-30-00.500.log",
			expectToInclude: true,
		},
		// Files before or equal to start time should be excluded
		{
			fileName:        "tidb-audit-2025-09-10T17-01-55.073.log",
			expectToInclude: false,
		},
		{
			fileName:        "tidb-audit-2025-09-10T17-01-56.000.log",
			expectToInclude: false,
		},
		// Invalid file names should be excluded
		{
			fileName:        "tidb-audit-invalid-timestamp.log",
			expectToInclude: false,
		},
		{
			fileName:        "traffic-1.log",
			expectToInclude: false,
		},
		{
			fileName:        "tidb-audit.log",
			expectToInclude: false,
		},
		{
			fileName:        "tidb-audit-2025-13-40T25-70-70.log",
			expectToInclude: false,
		},
	}
	expectedFileOrder := []string{
		"tidb-audit-2025-09-10T17-01-56.073.log",
		"tidb-audit-2025-09-10T17-01-56.172.log.gz",
		"tidb-audit-2025-09-11T10-30-00.500.log",
	}
	for i, test := range tests {
		included := filterFileByTime(test.fileName, auditFileNamePrefix, commandStartTime)
		require.Equal(t, test.expectToInclude, included, "case %d", i)
	}

	dir := t.TempDir()
	require.NoError(t, os.RemoveAll(dir))
	require.NoError(t, os.MkdirAll(dir, 0777))
	for _, test := range tests {
		f, err := os.Create(filepath.Join(dir, test.fileName))
		require.NoError(t, err)
		if strings.HasSuffix(test.fileName, ".gz") {
			w := gzip.NewWriter(f)
			_, err := w.Write([]byte{})
			require.NoError(t, err)
			require.NoError(t, w.Close())
		}
		require.NoError(t, f.Close())
	}
	storage, err := NewStorage(dir)
	require.NoError(t, err)
	defer storage.Close()
	lg, _ := logger.CreateLoggerForTest(t)
	l, err := newRotateReader(lg, storage, ReaderCfg{Dir: dir, Format: cmd.FormatAuditLogPlugin, CommandStartTime: commandStartTime})
	require.NoError(t, err)
	var fileOrder []string
	for {
		if err := l.nextReader(); err != nil {
			require.True(t, errors.Is(err, io.EOF))
			break
		}
		fileOrder = append(fileOrder, l.curFileName)
	}
	require.Equal(t, expectedFileOrder, fileOrder)
}

func TestWalkS3ForAuditLogFile(t *testing.T) {
	controller := gomock.NewController(t)
	s3api := mock.NewMockS3API(controller)

	s3api.EXPECT().ListObjectsWithContext(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *s3.ListObjectsInput, _ ...request.Option) (*s3.ListObjectsOutput, error) {
			require.Equal(t, "bucket", *req.Bucket)
			require.Equal(t, "prefix/tidb-audit-", *req.Prefix)

			return &s3.ListObjectsOutput{
				Contents: []*s3.Object{
					{Key: aws.String("prefix/tidb-audit-2025-09-15T16-54-44.939.log"), Size: aws.Int64(200)},
				},
			}, nil
		},
	)

	r := &rotateReader{
		cfg: ReaderCfg{
			Format:           cmd.FormatAuditLogPlugin,
			CommandStartTime: time.Time{},
		},
		curFileName: "",
	}
	err := r.walkS3ForAuditLogFile(context.Background(), s3api, &backuppb.S3{
		Bucket: "bucket",
		Prefix: "prefix/",
	}, func(fileName string, size int64) error {
		require.Equal(t, fileName, "tidb-audit-2025-09-15T16-54-44.939.log")

		return nil
	})
	require.NoError(t, err)
}
