// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"go.uber.org/zap"
)

var _ io.WriteCloser = (*rotateWriter)(nil)

type rotateWriter struct {
	cfg      WriterCfg
	writer   io.WriteCloser
	lg       *zap.Logger
	fileIdx  int
	writeLen int
}

func newRotateWriter(lg *zap.Logger, cfg WriterCfg) *rotateWriter {
	if cfg.FileSize == 0 {
		cfg.FileSize = fileSize
	}
	return &rotateWriter{
		cfg: cfg,
		lg:  lg,
	}
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
	path := filepath.Join(w.cfg.Dir, fileName)
	// compressWriter -> encryptWriter -> file
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return errors.WithStack(err)
	}
	if w.writer, err = newWriterWithEncryptOpts(file, w.cfg.EncryptMethod, w.cfg.KeyFile); err != nil {
		return err
	}
	if w.cfg.Compress {
		w.writer = newCompressWriter(w.lg, w.writer)
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
	cfg         ReaderCfg
	curFileName string
	curFileIdx  int
	curFile     *os.File
	reader      io.Reader
	lg          *zap.Logger
	eof         bool
}

func newRotateReader(lg *zap.Logger, cfg ReaderCfg) *rotateReader {
	return &rotateReader{
		cfg: cfg,
		lg:  lg,
	}
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
	if r.curFile != nil {
		if err := r.curFile.Close(); err != nil {
			r.lg.Warn("failed to close file", zap.String("filename", r.curFile.Name()), zap.Error(err))
			return err
		}
		r.curFile = nil
	}
	return nil
}

func (r *rotateReader) nextReader() error {
	files, err := os.ReadDir(r.cfg.Dir)
	if err != nil {
		return errors.WithStack(err)
	}
	var minFileIdx int
	var minFileName string
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		name := file.Name()
		if !strings.HasPrefix(name, fileNamePrefix) {
			continue
		}
		fileIdx := parseFileIdx(name)
		if fileIdx == 0 {
			r.lg.Warn("traffic file name is invalid", zap.String("filename", name))
			continue
		}
		if fileIdx <= r.curFileIdx {
			continue
		}
		if minFileName == "" || fileIdx < minFileIdx {
			minFileIdx = fileIdx
			minFileName = name
		}
	}
	if minFileName == "" {
		return io.EOF
	}
	fileReader, err := os.Open(filepath.Join(r.cfg.Dir, minFileName))
	if err != nil {
		return errors.WithStack(err)
	}
	r.curFile = fileReader
	r.curFileIdx = minFileIdx
	r.curFileName = minFileName
	// compressReader -> encryptReader -> file
	r.reader, err = newReaderWithEncryptOpts(fileReader, r.cfg.EncryptMethod, r.cfg.KeyFile)
	if err != nil {
		return err
	}
	if strings.HasSuffix(minFileName, fileCompressFormat) {
		if r.reader, err = newCompressReader(r.reader); err != nil {
			return err
		}
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
