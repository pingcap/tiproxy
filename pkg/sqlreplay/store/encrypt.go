// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pingcap/tiproxy/lib/util/errors"
)

const (
	EncryptPlain = "plaintext"
	EncryptAes   = "aes256-ctr"
)

var _ io.WriteCloser = (*aesCTRWriter)(nil)

type aesCTRWriter struct {
	io.WriteCloser
	stream cipher.Stream
}

func newWriterWithEncryptOpts(writer io.WriteCloser, encryptionMethod string, encrytionKey []byte) (io.WriteCloser, error) {
	switch strings.ToLower(encryptionMethod) {
	case "", EncryptPlain:
		return writer, nil
	case EncryptAes:
		return newAESCTRWriter(writer, encrytionKey)
	default:
		return nil, fmt.Errorf("unsupported encrypt method: %s", encryptionMethod)
	}
}

func newAESCTRWriter(writer io.WriteCloser, key []byte) (*aesCTRWriter, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, errors.WithStack(err)
	}
	ctr := &aesCTRWriter{
		WriteCloser: writer,
		stream:      cipher.NewCTR(block, iv),
	}
	_, err = ctr.WriteCloser.Write(iv)
	return ctr, err
}

func (ctr *aesCTRWriter) Write(data []byte) (int, error) {
	ctr.stream.XORKeyStream(data, data)
	return ctr.WriteCloser.Write(data)
}

var _ io.Reader = (*aesCTRReader)(nil)

type aesCTRReader struct {
	io.Reader
	stream cipher.Stream
}

func newReaderWithEncryptOpts(reader io.Reader, encryptionMethod string, encryptionKey []byte) (io.Reader, error) {
	switch strings.ToLower(encryptionMethod) {
	case "", EncryptPlain:
		return reader, nil
	case EncryptAes:
		return newAESCTRReader(reader, encryptionKey)
	default:
		return nil, fmt.Errorf("unsupported encrypt method: %s", encryptionMethod)
	}
}

func newAESCTRReader(reader io.Reader, key []byte) (*aesCTRReader, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	iv := make([]byte, aes.BlockSize)
	for readLen := 0; readLen < len(iv); {
		m, err := reader.Read(iv[readLen:])
		if err != nil {
			return nil, err
		}
		readLen += m
	}
	return &aesCTRReader{
		Reader: reader,
		stream: cipher.NewCTR(block, iv),
	}, nil
}

func (ctr *aesCTRReader) Read(data []byte) (int, error) {
	n, err := ctr.Reader.Read(data)
	if n > 0 {
		ctr.stream.XORKeyStream(data[:n], data[:n])
	}
	if errors.Is(err, io.EOF) {
		return n, err
	}
	return n, errors.WithStack(err)
}

func LoadEncryptionKey(encryptionMethod, keyFile string) ([]byte, error) {
	switch strings.ToLower(encryptionMethod) {
	case "", EncryptPlain:
		return nil, nil
	case EncryptAes:
		return readAesKey(keyFile)
	default:
		return nil, fmt.Errorf("unsupported encrypt method: %s", encryptionMethod)
	}
}

func readAesKey(filename string) ([]byte, error) {
	if len(filename) == 0 {
		return nil, errors.New("security.encryption-key-path is not set")
	}
	key, err := os.ReadFile(filename)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(key) < 32 {
		return nil, errors.Errorf("invalid aes-256 key length: %d, expecting 32", len(key))
	}
	// in case it's ended with a new line
	return key[:32], nil
}
