// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bufio"
	"fmt"
	"io"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"go.uber.org/zap"
)

type WriterCfg struct {
	Dir              string
	EncryptionMethod string
	EncryptionKey    []byte
	FileSize         int
	Compress         bool
}

// NewWriter just wraps the rotate writer. It doesn't use a buffer because Capture writes data in a big batch.
// Capture uses a bytes buffer to encode commands and the buffer can not be replaced with a bufio.Writer.
func NewWriter(lg *zap.Logger, externalStorage storage.ExternalStorage, cfg WriterCfg) (io.WriteCloser, error) {
	return newRotateWriter(lg, externalStorage, cfg)
}

type ReaderCfg struct {
	Dir              string
	Format           string
	EncryptionMethod string
	EncryptionKey    []byte
	// Reader will skip the files whose end time is before FileNameFilterTime.
	FileNameFilterTime time.Time
	WaitOnEOF          bool
}

var _ cmd.LineReader = (*loader)(nil)

type loader struct {
	cfg         ReaderCfg
	bufReader   *bufio.Reader
	reader      Reader
	curFileName string
	curLineIdx  int
	lg          *zap.Logger
}

func NewReader(lg *zap.Logger, storage storage.ExternalStorage, cfg ReaderCfg) (*loader, error) {
	reader, err := newRotateReader(lg, storage, cfg)
	if err != nil {
		return nil, err
	}
	return &loader{
		cfg:       cfg,
		lg:        lg,
		reader:    reader,
		bufReader: bufio.NewReaderSize(reader, bufferSize),
	}, nil
}

func (l *loader) String() string {
	return fmt.Sprintf("curFile: %s, curLineIdx: %d", l.curFileName, l.curLineIdx)
}

func (l *loader) Read(data []byte) (string, int, error) {
	for readLen := 0; readLen < len(data); {
		n, err := l.bufReader.Read(data[readLen:])
		if err != nil {
			return l.curFileName, l.curLineIdx, err
		}
		readLen += n
	}
	fileName := l.reader.CurFile()
	if fileName != l.curFileName {
		l.curFileName = fileName
		l.curLineIdx = 1
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
	var result, line []byte
	var err error
	for isPrefix := true; isPrefix; {
		if line, isPrefix, err = l.bufReader.ReadLine(); err != nil {
			break
		}
		// The returned line is a reference to the internal buffer of the reader, so copy it.
		result = append(result, line...)
	}
	fileName := l.reader.CurFile()
	if fileName != l.curFileName {
		l.curFileName = fileName
		l.curLineIdx = 1
	}
	curLineIdx := l.curLineIdx
	l.curLineIdx++
	return result, l.curFileName, curLineIdx, err
}

func (l *loader) Close() {
	l.reader.Close()
}
