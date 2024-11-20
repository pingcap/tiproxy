// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bufio"
	"compress/gzip"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"go.uber.org/zap"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Writer interface {
	Write([]byte) error
	Close() error
}

var _ Writer = (*rotateWriter)(nil)

type rotateWriter struct {
	lg *lumberjack.Logger
}

func newRotateWriter(cfg WriterCfg) *rotateWriter {
	if cfg.FileSize == 0 {
		cfg.FileSize = fileSize
	}
	return &rotateWriter{
		lg: &lumberjack.Logger{
			Filename:  filepath.Join(cfg.Dir, fileName),
			MaxSize:   cfg.FileSize,
			LocalTime: true,
			Compress:  true,
		},
	}
}

func (w *rotateWriter) Write(data []byte) error {
	_, err := w.lg.Write(data)
	return err
}

func (w *rotateWriter) Close() error {
	return w.lg.Close()
}

type Reader interface {
	Read([]byte) (int, error)
	CurFile() string
	Close()
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

func (r *rotateReader) Close() {
	if r.curFile != nil {
		if err := r.curFile.Close(); err != nil {
			r.lg.Warn("failed to close file", zap.String("filename", r.curFile.Name()), zap.Error(err))
		}
		r.curFile = nil
	}
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
		// traffic.log
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
