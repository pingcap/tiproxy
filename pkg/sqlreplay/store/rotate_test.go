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
	brstorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

type faultStorage struct {
	brstorage.ExternalStorage
	walkDir func(context.Context, *brstorage.WalkOption, func(string, int64) error) error
	open    func(context.Context, string, *brstorage.ReaderOption) (brstorage.ExternalFileReader, error)
}

func (s *faultStorage) WalkDir(ctx context.Context, opt *brstorage.WalkOption, fn func(string, int64) error) error {
	if s.walkDir != nil {
		return s.walkDir(ctx, opt, fn)
	}
	return s.ExternalStorage.WalkDir(ctx, opt, fn)
}

func (s *faultStorage) Open(ctx context.Context, path string, opt *brstorage.ReaderOption) (brstorage.ExternalFileReader, error) {
	if s.open != nil {
		return s.open(ctx, path, opt)
	}
	return s.ExternalStorage.Open(ctx, path, opt)
}

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
		time.Sleep(time.Millisecond)
	}
	require.NoError(t, writer.Close())
	require.Equal(t, 3, countTrafficFiles(t, tmpDir))
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
		require.NoError(t, writer.Close(), "case %d", i)
		files := listFiles(t, tmpDir)
		require.Len(t, files, 1, "case %d", i)
		require.True(t, strings.HasSuffix(files[0], test.ext), "case %d", i)
		require.NoError(t, os.Remove(filepath.Join(tmpDir, files[0])), "case %d", i)
	}
}

func TestParseFileTime(t *testing.T) {
	tests := []struct {
		fileName string
		fileTime time.Time
	}{
		{"tidb-audit-2025-09-10T17-01-56.073.log", mustParseTime("2025-09-10T17-01-56.073")},
		{"tidb-audit-2025-09-10T17-01-56.172.log.gz", mustParseTime("2025-09-10T17-01-56.172")},
		{"tidb-audit-2025-09-10T17-01-56.log.gz", mustParseTime("2025-09-10T17-01-56.000")},
		{"traffic-2025-09-10T17-01-56.172.log", time.Time{}},
		{"traffic-2025-09-10T17-01-56.172.log.gz", time.Time{}},
		{"tidb-audit-.log", time.Time{}},
		{"tidb-audit-.log.gz", time.Time{}},
		{"tidb-audit.log", time.Time{}},
		{"tidb-audit-100.gz", time.Time{}},
		{"test", time.Time{}},
		{"tidb-audit.log.gz", time.Time{}},
	}

	for i, test := range tests {
		ts := parseFileTime(test.fileName, auditFileNamePrefix)
		if test.fileTime.IsZero() {
			require.True(t, ts.IsZero(), "case %d", i)
		} else {
			require.True(t, ts.Equal(test.fileTime), "case %d: expected %v, got %v", i, test.fileTime, ts)
		}
	}
}

func mustParseTime(s string) time.Time {
	t, err := time.ParseInLocation(logTimeLayout, s, time.Local)
	if err != nil {
		panic(err)
	}
	return t
}

func TestIterateFiles(t *testing.T) {
	tests := []struct {
		format    cmd.TrafficFormat
		fileNames []string
		order     []string
	}{
		{
			fileNames: []string{},
			order:     []string{},
		},
		{
			fileNames: []string{
				"traffic-2025-09-10T17-01-56.073.log.gz",
			},
			order: []string{
				"traffic-2025-09-10T17-01-56.073.log.gz",
			},
		},
		{
			fileNames: []string{
				"traffic-2025-09-10T17-01-56.172.log.gz",
				"traffic-2025-09-10T17-01-56.073.log.gz",
			},
			order: []string{
				"traffic-2025-09-10T17-01-56.073.log.gz",
				"traffic-2025-09-10T17-01-56.172.log.gz",
			},
		},
		{
			fileNames: []string{
				"traffic-2025-09-10T17-01-56.073.log",
				"traffic-2025-09-10T17-01-56.172.log",
			},
			order: []string{
				"traffic-2025-09-10T17-01-56.073.log",
				"traffic-2025-09-10T17-01-56.172.log",
			},
		},
		{
			fileNames: []string{
				"traffic-2025-09-10T17-01-56.073.log.gz",
				"traffic-2025-09-10T17-01-56.172.log.gz",
				"traffic.log",
				"meta",
				"dir",
			},
			order: []string{
				"traffic-2025-09-10T17-01-56.073.log.gz",
				"traffic-2025-09-10T17-01-56.172.log.gz",
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
			fileOrder = append(fileOrder, l.externalFile.fileName)
		}
		require.Equal(t, test.order, fileOrder)
	}
}

func TestWaitOnEOF(t *testing.T) {
	dir := t.TempDir()
	storage, err := NewStorage(dir)
	require.NoError(t, err)
	defer storage.Close()
	l, err := newRotateReader(zap.NewNop(), storage, ReaderCfg{Dir: dir, Format: cmd.FormatAuditLogPlugin, WaitOnEOF: true})
	require.NoError(t, err)

	// Read next file when no available files.
	fileName := "tidb-audit-2025-09-10T17-01-56.073.log"
	fileCh := make(chan string)
	var wg waitgroup.WaitGroup
	wg.Run(func() {
		if err := l.nextReader(); err != nil {
			require.True(t, errors.Is(err, io.EOF))
		} else {
			fileCh <- l.externalFile.fileName
		}
	}, nil)

	// Wait for a while and then create the file.
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, os.WriteFile(filepath.Join(dir, fileName), []byte{}, 0666))
	require.Equal(t, fileName, <-fileCh)
	require.NoError(t, l.Close())
	wg.Wait()
}

func TestRecoversFromWalkError(t *testing.T) {
	for _, waitOnEOF := range []bool{false, true} {
		t.Run(fmt.Sprintf("wait_on_eof_%t", waitOnEOF), func(t *testing.T) {
			testRecoversFromWalkError(t, waitOnEOF)
		})
	}
}

func testRecoversFromWalkError(t *testing.T, waitOnEOF bool) {
	dir := t.TempDir()
	baseStorage, err := NewStorage(dir)
	require.NoError(t, err)
	defer baseStorage.Close()

	walkFailed := make(chan struct{})
	firstAttempt := true
	storage := &faultStorage{ExternalStorage: baseStorage}
	storage.walkDir = func(ctx context.Context, opt *brstorage.WalkOption, fn func(string, int64) error) error {
		if firstAttempt {
			firstAttempt = false
			close(walkFailed)
			return context.DeadlineExceeded
		}
		return baseStorage.WalkDir(ctx, opt, fn)
	}
	l := &rotateReader{
		cfg: ReaderCfg{
			Dir:       dir,
			Format:    cmd.FormatAuditLogPlugin,
			WaitOnEOF: waitOnEOF,
		},
		storage: storage,
		lg:      zap.NewNop(),
		fileCh:  make(chan fileReader, 1),
	}
	loopErr := startRotateReader(l)

	nextReaderErr := make(chan error, 1)
	go func() {
		nextReaderErr <- l.nextReader()
	}()
	select {
	case <-walkFailed:
	case <-time.After(time.Second):
		t.Fatal("reader did not encounter the injected storage error")
	}

	fileName := "tidb-audit-2025-09-10T17-01-56.073.log"
	require.NoError(t, os.WriteFile(filepath.Join(dir, fileName), nil, 0666))
	select {
	case err := <-nextReaderErr:
		require.NoError(t, err)
		require.Equal(t, fileName, l.externalFile.fileName)
	case <-time.After(3 * time.Second):
		t.Fatal("reader did not recover after the storage error")
	}
	if waitOnEOF {
		require.NoError(t, l.Close())
		require.NoError(t, <-loopErr)
	} else {
		require.ErrorIs(t, <-loopErr, io.EOF)
		require.NoError(t, l.Close())
	}
}

func TestWaitOnEOFRetriesSameFileAfterOpenError(t *testing.T) {
	dir := t.TempDir()
	baseStorage, err := NewStorage(dir)
	require.NoError(t, err)
	defer baseStorage.Close()

	firstFile := "tidb-audit-2025-09-10T17-01-56.073.log"
	secondFile := "tidb-audit-2025-09-10T17-01-57.073.log"
	require.NoError(t, os.WriteFile(filepath.Join(dir, firstFile), nil, 0666))
	require.NoError(t, os.WriteFile(filepath.Join(dir, secondFile), nil, 0666))
	walkCount := 0
	walkCountAtOpenRetry := make(chan int, 1)
	openFailed := make(chan struct{})
	openCount := 0
	storage := &faultStorage{ExternalStorage: baseStorage}
	storage.walkDir = func(ctx context.Context, opt *brstorage.WalkOption, fn func(string, int64) error) error {
		walkCount++
		return baseStorage.WalkDir(ctx, opt, fn)
	}
	storage.open = func(ctx context.Context, path string, opt *brstorage.ReaderOption) (brstorage.ExternalFileReader, error) {
		openCount++
		if openCount == 1 {
			close(openFailed)
			return nil, context.DeadlineExceeded
		}
		if openCount == 2 {
			walkCountAtOpenRetry <- walkCount
		}
		return baseStorage.Open(ctx, path, opt)
	}
	l := &rotateReader{
		cfg: ReaderCfg{
			Dir:       dir,
			Format:    cmd.FormatAuditLogPlugin,
			WaitOnEOF: true,
		},
		storage: storage,
		lg:      zap.NewNop(),
		fileCh:  make(chan fileReader, 1),
	}
	loopErr := startRotateReader(l)

	nextReaderErr := make(chan error, 1)
	go func() {
		nextReaderErr <- l.nextReader()
	}()
	select {
	case <-openFailed:
	case <-time.After(time.Second):
		t.Fatal("reader did not encounter the injected open error")
	}
	select {
	case count := <-walkCountAtOpenRetry:
		require.Equal(t, 1, count)
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not retry opening the file")
	}

	select {
	case err := <-nextReaderErr:
		require.NoError(t, err)
		require.Equal(t, firstFile, l.externalFile.fileName)
	case <-time.After(3 * time.Second):
		t.Fatal("reader did not recover after the open error")
	}
	require.NoError(t, l.Close())
	require.NoError(t, <-loopErr)
}

func startRotateReader(l *rotateReader) <-chan error {
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	loopErr := make(chan error, 1)
	l.wg.Run(func() {
		loopErr <- l.openFileLoop(ctx)
	}, l.lg)
	return loopErr
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
		require.NoError(t, writer.Close())
		files := listFiles(t, tmpDir)
		for _, f := range files {
			require.True(t, strings.HasPrefix(f, fileNamePrefix))
			if compress {
				require.True(t, strings.HasSuffix(f, fileCompressFormat))
			} else {
				require.True(t, strings.HasSuffix(f, fileNameSuffix))
			}
		}

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
	files := listFiles(t, tmpDir)
	require.Len(t, files, 2)
	for _, name := range files {
		require.True(t, strings.HasPrefix(name, fileNamePrefix))
		require.True(t, strings.HasSuffix(name, fileCompressFormat))
		file, err := os.Open(filepath.Join(tmpDir, name))
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
	l, err := newRotateReader(lg, storage, ReaderCfg{
		Dir:                dir,
		Format:             cmd.FormatAuditLogPlugin,
		FileNameFilterTime: commandStartTime,
	})
	require.NoError(t, err)
	var fileOrder []string
	for {
		if err := l.nextReader(); err != nil {
			require.True(t, errors.Is(err, io.EOF))
			break
		}
		fileOrder = append(fileOrder, l.externalFile.fileName)
	}
	require.Equal(t, expectedFileOrder, fileOrder)
}

func TestWalkS3(t *testing.T) {
	controller := gomock.NewController(t)
	s3api := mock.NewMockS3API(controller)

	var files []*s3.Object
	// Append 1000 files
	for i := range 1000 {
		files = append(files, &s3.Object{
			Key:  aws.String(fmt.Sprintf("prefix/tidb-audit-2025-09-19T16-54-44.%03d.log", i)),
			Size: aws.Int64(200),
		})
	}
	for i := range 1000 {
		files = append(files, &s3.Object{
			Key:  aws.String(fmt.Sprintf("prefix/tidb-audit-2025-09-19T16-54-45.%03d.log", i)),
			Size: aws.Int64(200),
		})
	}

	// First request: return first 1000 files
	// Second request: from 44.999 to 45.999, return next 1000 files
	// Third request: from 45.998 to end, return 2 files
	// Fourth request: from 45.999 end, return 1 file
	s3api.EXPECT().ListObjectsWithContext(gomock.Any(), gomock.Any()).MaxTimes(4).DoAndReturn(
		func(ctx context.Context, req *s3.ListObjectsInput, _ ...request.Option) (*s3.ListObjectsOutput, error) {
			require.Equal(t, "bucket", *req.Bucket)
			require.Equal(t, "prefix/tidb-audit-", *req.Prefix)
			retFiles := files
			for i := range files {
				if *files[i].Key >= *req.Marker {
					retFiles = files[i:]
					break
				}
			}
			if len(retFiles) > int(*req.MaxKeys) {
				retFiles = retFiles[:*req.MaxKeys]
			}

			return &s3.ListObjectsOutput{
				Contents: retFiles,
			}, nil
		},
	)

	r := &rotateReader{
		cfg: ReaderCfg{
			Format:             cmd.FormatAuditLogPlugin,
			FileNameFilterTime: time.Time{},
		},
	}
	selectedFileCount := 0
	curFilename := ""
	for {
		selected := false
		cFileName := curFilename
		err := r.walkS3(context.Background(), cFileName, s3api, &backuppb.S3{
			Bucket: "bucket",
			Prefix: "prefix/",
		}, func(fileName string, size int64) (bool, error) {
			require.GreaterOrEqual(t, fileName, cFileName)
			if fileName <= cFileName {
				return false, nil
			}

			curFilename = fileName
			selected = true
			return true, nil
		})
		require.NoError(t, err)

		if !selected {
			break
		}
		selectedFileCount++
	}
	// Iterate through the whole 2000 files
	require.Equal(t, 2000, selectedFileCount)
}
