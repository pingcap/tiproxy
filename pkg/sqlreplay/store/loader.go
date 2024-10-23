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
	"strings"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"go.uber.org/zap"
)

type LoaderCfg struct {
	Dir string
}

var _ cmd.LineReader = (*loader)(nil)

type loader struct {
	cfg LoaderCfg
	// the time in the file name
	curFileName string
	curFileTs   int64
	curLineIdx  int
	curFile     *os.File
	reader      *bufio.Reader
	lg          *zap.Logger
}

func NewLoader(lg *zap.Logger, cfg LoaderCfg) *loader {
	return &loader{
		cfg: cfg,
		lg:  lg,
	}
}

func (l *loader) String() string {
	return fmt.Sprintf("curFile: %s, curLineIdx: %d", l.curFileName, l.curLineIdx)
}

func (l *loader) Read(data []byte) (string, int, error) {
	if l.reader == nil {
		if err := l.nextReader(); err != nil {
			return l.curFileName, l.curLineIdx, err
		}
	}

	for readLen := 0; readLen < len(data); {
		m, err := l.reader.Read(data[readLen:])
		if err != nil {
			if err == io.EOF {
				readLen += m
				if err = l.nextReader(); err != nil {
					return l.curFileName, l.curLineIdx, err
				}
				continue
			}
			return l.curFileName, l.curLineIdx, errors.WithStack(err)
		}
		readLen += m
	}
	curLineIdx := l.curLineIdx
	for _, b := range data {
		if b == '\n' {
			l.curLineIdx++
		}
	}
	return l.curFileName, curLineIdx, nil
}

func (l *loader) ReadLine() ([]byte, string, int, error) {
	if l.reader == nil {
		if err := l.nextReader(); err != nil {
			return nil, l.curFileName, l.curLineIdx, err
		}
	}

	isPrefix := true
	var result, line []byte
	var err error
	for isPrefix {
		if line, isPrefix, err = l.reader.ReadLine(); err != nil {
			if err == io.EOF {
				if err = l.nextReader(); err != nil {
					return nil, l.curFileName, l.curLineIdx, err
				}
				isPrefix = true
				continue
			}
			return nil, l.curFileName, l.curLineIdx, errors.WithStack(err)
		}
		// The returned line is a reference to the internal buffer of the reader, so copy it.
		result = append(result, line...)
	}
	l.curLineIdx++
	return result, l.curFileName, l.curLineIdx - 1, nil
}

func (l *loader) nextReader() error {
	// has read the latest file
	if l.curFileTs == math.MaxInt64 {
		return io.EOF
	}
	files, err := os.ReadDir(l.cfg.Dir)
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
			l.lg.Warn("traffic file name is invalid", zap.String("filename", name))
			continue
		}
		if fileTs <= l.curFileTs {
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
	r, err := os.Open(filepath.Join(l.cfg.Dir, minFileName))
	if err != nil {
		return errors.WithStack(err)
	}
	if l.curFile != nil {
		if err := l.curFile.Close(); err != nil {
			l.lg.Warn("failed to close file", zap.String("filename", l.curFile.Name()), zap.Error(err))
		}
	}
	l.curFile = r
	l.curFileTs = minFileTs
	l.curFileName = minFileName
	l.curLineIdx = 1
	if strings.HasSuffix(minFileName, fileCompressFormat) {
		gr, err := gzip.NewReader(r)
		if err != nil {
			return errors.WithStack(err)
		}
		l.reader = bufio.NewReader(gr)
	} else {
		l.reader = bufio.NewReader(r)
	}
	l.lg.Info("reading next file", zap.String("file", minFileName))
	return nil
}

func (l *loader) Close() {
	if l.curFile != nil {
		if err := l.curFile.Close(); err != nil {
			l.lg.Warn("failed to close file", zap.String("filename", l.curFile.Name()), zap.Error(err))
		}
		l.curFile = nil
	}
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
