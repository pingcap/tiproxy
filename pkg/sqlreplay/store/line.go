// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"go.uber.org/zap"
)

const (
	EncryptPlain = "plaintext"
	EncryptAes   = "aes256-ctr"
)

type WriterCfg struct {
	Dir           string
	EncryptMethod string
	KeyFile       string
	FileSize      int
	Compress      bool
}

func NewWriter(lg *zap.Logger, cfg WriterCfg) (Writer, error) {
	rotateWriter := newRotateWriter(lg, cfg)
	encryptMethod := strings.ToLower(cfg.EncryptMethod)
	switch encryptMethod {
	case "", EncryptPlain:
		return rotateWriter, nil
	case EncryptAes:
		return newAESCTRWriter(rotateWriter, cfg.KeyFile)
	default:
		return nil, fmt.Errorf("unsupported encrypt method: %s", encryptMethod)
	}
}

type ReaderCfg struct {
	Dir           string
	EncryptMethod string
	KeyFile       string
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

func NewReader(lg *zap.Logger, cfg ReaderCfg) (*loader, error) {
	var reader Reader
	reader = newRotateReader(lg, cfg.Dir)
	encryptMethod := strings.ToLower(cfg.EncryptMethod)
	switch encryptMethod {
	case "", EncryptPlain:
	case EncryptAes:
		var err error
		reader, err = newAESCTRReader(reader, cfg.KeyFile)
		if err != nil {
			return nil, err
		}
	}
	return &loader{
		cfg:       cfg,
		lg:        lg,
		reader:    reader,
		bufReader: bufio.NewReader(reader),
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
