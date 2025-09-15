// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"go.uber.org/zap"
)

var _ io.WriteCloser = (*rotateWriter)(nil)

type rotateWriter struct {
	cfg      WriterCfg
	writer   io.WriteCloser
	storage  storage.ExternalStorage
	lg       *zap.Logger
	fileIdx  int
	writeLen int
}

func newRotateWriter(lg *zap.Logger, externalStorage storage.ExternalStorage, cfg WriterCfg) (*rotateWriter, error) {
	if cfg.FileSize == 0 {
		cfg.FileSize = fileSize
	}
	return &rotateWriter{
		cfg:     cfg,
		lg:      lg,
		storage: externalStorage,
	}, nil
}

func (w *rotateWriter) Write(data []byte) (n int, err error) {
	if w.writer == nil || reflect.ValueOf(w.writer).IsNil() {
		if err = w.createFile(); err != nil {
			return
		}
	}
	if n, err = w.writer.Write(data); err != nil {
		return n, errors.WithStack(err)
	}
	w.writeLen += n
	if w.writeLen >= w.cfg.FileSize {
		err = w.closeFile()
		w.writeLen = 0
	}
	return n, err
}

func (w *rotateWriter) createFile() error {
	var ext string
	if w.cfg.Compress {
		ext = fileCompressFormat
	}
	w.fileIdx++
	fileName := fmt.Sprintf("%s%d%s%s", fileNamePrefix, w.fileIdx, fileNameSuffix, ext)
	// rotateWriter -> encryptWriter -> compressWriter -> file
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	fileWriter, err := w.storage.Create(ctx, fileName, &storage.WriterOption{})
	cancel()
	w.writer = NewStorageWriter(fileWriter)
	if err != nil {
		return errors.WithStack(err)
	}
	if w.cfg.Compress {
		w.writer = newCompressWriter(w.lg, w.writer)
	}
	if w.writer, err = newWriterWithEncryptOpts(w.writer, w.cfg.EncryptionMethod, w.cfg.EncryptionKey); err != nil {
		return err
	}
	return nil
}

func (w *rotateWriter) closeFile() error {
	if w.writer != nil && !reflect.ValueOf(w.writer).IsNil() {
		err := w.writer.Close()
		w.writer = nil
		return err
	}
	return nil
}

func (w *rotateWriter) Close() error {
	return w.closeFile()
}

type Reader interface {
	io.ReadCloser
	CurFile() string
}

var _ Reader = (*rotateReader)(nil)

type rotateReader struct {
	cfg          ReaderCfg
	curFileName  string
	curFileIdx   int64
	reader       io.Reader
	externalFile storage.ExternalFileReader
	storage      storage.ExternalStorage
	lg           *zap.Logger
	eof          bool
}

func newRotateReader(lg *zap.Logger, storage storage.ExternalStorage, cfg ReaderCfg) (*rotateReader, error) {
	return &rotateReader{
		cfg:     cfg,
		lg:      lg,
		storage: storage,
	}, nil
}

func (r *rotateReader) Read(data []byte) (int, error) {
	if r.eof {
		return 0, io.EOF
	}
	if r.reader == nil || reflect.ValueOf(r.reader).IsNil() {
		if err := r.nextReader(); err != nil {
			return 0, err
		}
	}

	for {
		m, err := r.reader.Read(data[:])
		if err == nil {
			return m, nil
		}
		if !errors.Is(err, io.EOF) {
			return m, errors.WithStack(err)
		}
		_ = r.Close()
		if err := r.nextReader(); err != nil {
			r.eof = true
			return m, err
		}
		if m > 0 {
			return m, nil
		}
	}
}

func (r *rotateReader) CurFile() string {
	return r.curFileName
}

func (r *rotateReader) Close() error {
	if r.externalFile != nil && !reflect.ValueOf(r.externalFile).IsNil() {
		if err := r.externalFile.Close(); err != nil {
			r.lg.Warn("failed to close file", zap.String("filename", r.curFileName), zap.Error(err))
			return err
		}
		r.externalFile = nil
	}
	return nil
}

func (r *rotateReader) nextReader() error {
	var minFileIdx int64
	var minFileName string
	fileNamePrefix := getFileNamePrefix(r.cfg.Format)
	parseFunc := getParseFileNameFunc(r.cfg.Format)
	fileFilter := getFilterFileNameFunc(r.cfg.Format, r.cfg.CommandStartTime)
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	err := r.storage.WalkDir(ctx, &storage.WalkOption{},
		func(name string, size int64) error {
			if !strings.HasPrefix(name, fileNamePrefix) {
				return nil
			}
			if !fileFilter(name, fileNamePrefix) {
				return nil
			}
			fileIdx := parseFunc(name, fileNamePrefix)
			if fileIdx == 0 {
				r.lg.Warn("traffic file name is invalid", zap.String("filename", name), zap.String("format", r.cfg.Format))
				return nil
			}
			if fileIdx <= r.curFileIdx {
				return nil
			}
			if minFileName == "" || fileIdx < minFileIdx {
				minFileIdx = fileIdx
				minFileName = name
			}
			return nil
		})
	cancel()
	if err != nil {
		return err
	}
	if minFileName == "" {
		return io.EOF
	}
	// storage.Open(ctx) stores the context internally for subsequent reads, so don't set a short timeout.
	fileReader, err := r.storage.Open(context.Background(), minFileName, &storage.ReaderOption{})
	if err != nil {
		return errors.WithStack(err)
	}
	r.externalFile = fileReader
	r.reader = fileReader
	r.curFileIdx = minFileIdx
	r.curFileName = minFileName
	// rotateReader -> encryptReader -> compressReader -> file
	if strings.HasSuffix(minFileName, fileCompressFormat) {
		if r.reader, err = newCompressReader(r.reader); err != nil {
			return err
		}
	}
	r.reader, err = newReaderWithEncryptOpts(r.reader, r.cfg.EncryptionMethod, r.cfg.EncryptionKey)
	if err != nil {
		return err
	}
	r.lg.Info("reading next file", zap.String("file", minFileName))
	return nil
}

func getFileNamePrefix(format string) string {
	switch format {
	case cmd.FormatAuditLogPlugin:
		return auditFileNamePrefix
	}
	return fileNamePrefix
}

func getParseFileNameFunc(format string) func(string, string) int64 {
	switch format {
	case cmd.FormatAuditLogPlugin:
		return parseFileTimeToIdx
	}
	return parseFileIdx
}

func getFilterFileNameFunc(format string, commandStartTime time.Time) func(string, string) bool {
	switch format {
	case cmd.FormatAuditLogPlugin:
		return func(name, fileNamePrefix string) bool {
			return filterFileByTime(name, fileNamePrefix, commandStartTime)
		}
	}
	return func(string, string) bool { return true }
}

// Parse the file name to get the file index.
// filename pattern: traffic-1.log.gz
func parseFileIdx(name, fileNamePrefix string) int64 {
	if !strings.HasPrefix(name, fileNamePrefix) {
		return 0
	}
	startIdx := len(fileNamePrefix)
	if len(name) <= startIdx+len(fileNameSuffix) {
		return 0
	}
	endIdx := len(name)
	if strings.HasSuffix(name, fileCompressFormat) {
		endIdx -= len(fileCompressFormat)
	}
	if !strings.HasSuffix(name[:endIdx], fileNameSuffix) {
		return 0
	}
	endIdx -= len(fileNameSuffix)
	fileIdx, err := strconv.Atoi(name[startIdx:endIdx])
	if err != nil {
		return 0
	}
	return int64(fileIdx)
}

// Parse the file name to get the file timestamp.
// filename pattern: tidb-audit-2025-09-10T17-01-56.073.log
func parseFileTime(name, fileNamePrefix string) time.Time {
	if !strings.HasPrefix(name, fileNamePrefix) {
		return time.Time{}
	}
	startIdx := len(fileNamePrefix)
	if len(name) <= startIdx+len(fileNameSuffix) {
		return time.Time{}
	}
	endIdx := len(name)
	if strings.HasSuffix(name, fileCompressFormat) {
		endIdx -= len(fileCompressFormat)
	}
	if !strings.HasSuffix(name[:endIdx], fileNameSuffix) {
		return time.Time{}
	}
	endIdx -= len(fileNameSuffix)
	// The `TimeZone` part is not included in the audit log file name, so we use the `time.Local` here.
	// It's always possible to workaround it by adjusting the commandStartTime, so just using `time.Local`
	// here is acceptable. Using the timezone from `commandStartTime` is another option, but it's a bit tricky.
	ts, err := time.ParseInLocation(logTimeLayout, name[startIdx:endIdx], time.Local)
	if err != nil {
		return time.Time{}
	}
	return ts
}

func parseFileTimeToIdx(name, fileNamePrefix string) int64 {
	ts := parseFileTime(name, fileNamePrefix)
	if ts.IsZero() {
		return 0
	}
	return ts.UnixNano() / 1000000
}

func filterFileByTime(name, fileNamePrefix string, commandStartTime time.Time) bool {
	fileTime := parseFileTime(name, fileNamePrefix)
	if fileTime.IsZero() {
		return false
	}
	// Be careful that the log file name doesn't contain timezone info.
	// We assume the log file time is the Local time. But anyway we could workaround it by
	// adjusting the commandStartTime.
	return fileTime.After(commandStartTime)
}
