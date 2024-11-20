// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
	"os"
	"reflect"

	"github.com/pingcap/tiproxy/lib/util/errors"
)

var _ Writer = (*aesCTRWriter)(nil)

type aesCTRWriter struct {
	Writer
	stream cipher.Stream
	iv     []byte
	inited bool
}

func newAESCTRWriter(writer Writer, keyFile string) (*aesCTRWriter, error) {
	key, err := readAesKey(keyFile)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, errors.WithStack(err)
	}
	return &aesCTRWriter{
		Writer: writer,
		stream: cipher.NewCTR(block, iv),
		iv:     iv,
	}, nil
}

func (ctr *aesCTRWriter) Write(data []byte) error {
	if !ctr.inited {
		if err := ctr.writeIV(); err != nil {
			return err
		}
		ctr.inited = true
	}
	ctr.stream.XORKeyStream(data, data)
	return ctr.Writer.Write(data)
}

func (ctr *aesCTRWriter) writeIV() error {
	return ctr.Writer.Write(ctr.iv)
}

func (ctr *aesCTRWriter) Close() error {
	return ctr.Writer.Close()
}

var _ Reader = (*aesCTRReader)(nil)

type aesCTRReader struct {
	Reader
	stream cipher.Stream
	key    []byte
}

func newAESCTRReader(reader Reader, keyFile string) (*aesCTRReader, error) {
	key, err := readAesKey(keyFile)
	if err != nil {
		return nil, err
	}
	return &aesCTRReader{
		Reader: reader,
		key:    key,
	}, nil
}

func (ctr *aesCTRReader) Read(data []byte) (int, error) {
	if ctr.stream == nil || reflect.ValueOf(ctr.stream).IsNil() {
		if err := ctr.init(); err != nil {
			return 0, err
		}
	}
	n, err := ctr.Reader.Read(data)
	if n > 0 {
		ctr.stream.XORKeyStream(data[:n], data[:n])
	}
	if err != nil {
		return n, err
	}
	return n, nil
}

func (ctr *aesCTRReader) init() error {
	block, err := aes.NewCipher(ctr.key)
	if err != nil {
		return errors.WithStack(err)
	}
	iv := make([]byte, aes.BlockSize)
	for readLen := 0; readLen < len(iv); {
		m, err := ctr.Reader.Read(iv[readLen:])
		if err != nil {
			return err
		}
		readLen += m
	}
	ctr.stream = cipher.NewCTR(block, iv)
	return nil
}

func (ctr *aesCTRReader) CurFile() string {
	return ctr.Reader.CurFile()
}

func (ctr *aesCTRReader) Close() {
	ctr.Reader.Close()
}

func readAesKey(filename string) ([]byte, error) {
	key, err := os.ReadFile(filename)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(key) != 32 {
		return nil, errors.Errorf("invalid aes-256 key length: %d, expecting 32", len(key))
	}
	return key, nil
}
