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

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiproxy/lib/util/errors"
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
	if w.writer, err = newWriterWithEncryptOpts(w.writer, w.cfg.EncryptMethod, w.cfg.KeyFile); err != nil {
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
	curFileIdx   int
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
		if err != io.EOF {
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
	var minFileIdx int
	var minFileName string
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	err := r.storage.WalkDir(ctx, &storage.WalkOption{},
		func(name string, size int64) error {
			if !strings.HasPrefix(name, fileNamePrefix) {
				return nil
			}
			fileIdx := parseFileIdx(name)
			if fileIdx == 0 {
				r.lg.Warn("traffic file name is invalid", zap.String("filename", name))
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
	ctx, cancel = context.WithTimeout(context.Background(), opTimeout)
	fileReader, err := r.storage.Open(ctx, minFileName, &storage.ReaderOption{})
	cancel()
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
	r.reader, err = newReaderWithEncryptOpts(r.reader, r.cfg.EncryptMethod, r.cfg.KeyFile)
	if err != nil {
		return err
	}
	r.lg.Info("reading next file", zap.String("file", minFileName))
	return nil
}

// Parse the file name to get the file index.
// filename pattern: traffic-1.log.gz
func parseFileIdx(name string) int {
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
	return fileIdx
}
