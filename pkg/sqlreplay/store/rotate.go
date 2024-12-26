// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"go.uber.org/zap"
)

type Writer interface {
	io.WriteCloser
}

var _ Writer = (*rotateWriter)(nil)

type rotateWriter struct {
	cfg       WriterCfg
	writer    io.WriteCloser
	file      *os.File
	lg        *zap.Logger
	lastMilli int64
	writeLen  int
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
	// Get a different time if the current time is the same as the last time.
	now := time.Now()
	if millis := now.UnixMilli(); millis <= w.lastMilli {
		w.lastMilli += 1
		now = time.Unix(w.lastMilli/1e3, (w.lastMilli%1e3)*1e6)
	} else {
		w.lastMilli = millis
	}
	t := now.Format(fileTsLayout)

	var ext string
	if w.cfg.Compress {
		ext = fileCompressFormat
	}
	fileName := fmt.Sprintf("%s-%s%s%s", fileNamePrefix, t, fileNameSuffix, ext)
	path := filepath.Join(w.cfg.Dir, fileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return errors.WithStack(err)
	}
	w.file = file
	if w.cfg.Compress {
		w.writer = gzip.NewWriter(file)
	} else {
		w.writer = file
	}
	return nil
}

func (w *rotateWriter) closeFile() error {
	var err error
	if w.writer != nil && !reflect.ValueOf(w.writer).IsNil() {
		if err = w.writer.Close(); err != nil {
			w.lg.Warn("failed to close writer", zap.Error(err))
		}
		w.writer = nil
	}
	if w.cfg.Compress && w.file != nil {
		if err = w.file.Close(); err != nil {
			w.lg.Warn("failed to close traffic file", zap.Error(err))
		}
	}
	w.file = nil
	return err
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
	dir string
	// the time in the file name
	curFileName string
	curFileTs   int64
	curFile     *os.File
	reader      *bufio.Reader
	lg          *zap.Logger
}

func newRotateReader(lg *zap.Logger, dir string) *rotateReader {
	return &rotateReader{
		dir: dir,
		lg:  lg,
	}
}

func (r *rotateReader) Read(data []byte) (int, error) {
	if r.reader == nil {
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
		if err := r.nextReader(); err != nil {
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
	// has read the latest file
	if r.curFileTs == math.MaxInt64 {
		return io.EOF
	}
	files, err := os.ReadDir(r.dir)
	if err != nil {
		return errors.WithStack(err)
	}
	var minFileTs int64
	var minFileName string
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		name := file.Name()
		if !strings.HasPrefix(name, fileNamePrefix) {
			continue
		}
		// traffic.log (used for lumberjack before, not used anymore)
		if name == fileName {
			if minFileName == "" {
				minFileTs = math.MaxInt64
				minFileName = name
			}
			continue
		}
		// rotated traffic file
		fileTs := parseFileTs(name)
		if fileTs == 0 {
			r.lg.Warn("traffic file name is invalid", zap.String("filename", name))
			continue
		}
		if fileTs <= r.curFileTs {
			continue
		}
		if minFileName == "" || fileTs < minFileTs {
			minFileTs = fileTs
			minFileName = name
		}
	}
	if minFileName == "" {
		return io.EOF
	}
	fileReader, err := os.Open(filepath.Join(r.dir, minFileName))
	if err != nil {
		return errors.WithStack(err)
	}
	if r.curFile != nil {
		if err := r.curFile.Close(); err != nil {
			r.lg.Warn("failed to close file", zap.String("filename", r.curFile.Name()), zap.Error(err))
		}
	}
	r.curFile = fileReader
	r.curFileTs = minFileTs
	r.curFileName = minFileName
	if strings.HasSuffix(minFileName, fileCompressFormat) {
		gr, err := gzip.NewReader(fileReader)
		if err != nil {
			return errors.WithStack(err)
		}
		r.reader = bufio.NewReader(gr)
	} else {
		r.reader = bufio.NewReader(fileReader)
	}
	r.lg.Info("reading next file", zap.String("file", minFileName))
	return nil
}

// Parse the file name to get the file time.
// filename pattern: traffic-2024-08-29T17-37-12.477.log.gz
// Ordering by string should also work.
func parseFileTs(name string) int64 {
	startIdx := len(fileNamePrefix)
	if len(name) <= startIdx {
		return 0
	}
	if name[startIdx] != '-' {
		return 0
	}
	startIdx++
	endIdx := len(name)
	if strings.HasSuffix(name, fileCompressFormat) {
		endIdx -= 3
	}
	if !strings.HasSuffix(name[:endIdx], fileNameSuffix) {
		return 0
	}
	endIdx -= len(fileNameSuffix)
	timeStr := name[startIdx:endIdx]
	t, err := time.Parse(fileTsLayout, timeStr)
	if err != nil {
		return 0
	}
	return t.UnixMilli()
}
