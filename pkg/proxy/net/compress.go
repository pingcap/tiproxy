// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"bytes"
	"compress/zlib"
	"io"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"go.uber.org/zap"
)

// CompressAlgorithm is the algorithm for MySQL compressed protocol.
type CompressAlgorithm int

const (
	// CompressionNone indicates no compression in use.
	CompressionNone CompressAlgorithm = iota
	// CompressionZlib is zlib/deflate.
	CompressionZlib
	// CompressionZstd is Facebook's Zstandard.
	CompressionZstd
)

const (
	// maxCompressedSize is the max uncompressed data size for a compressed packet.
	// Packets bigger than maxCompressedSize will be split into multiple compressed packets.
	// MySQL has 16K for the first packet. The rest packets and MySQL Connector/J are 16M.
	// It should be smaller than 16M so that the length can fit in the 3 byte field in the header.
	maxCompressedSize = 1<<24 - 1
	// minCompressSize is the min uncompressed data size for compressed data.
	// Packets smaller than minCompressSize won't be compressed.
	// MySQL and MySQL Connector/J are both 50.
	minCompressSize = 50
	// defaultZlibLevel is the compression level for zlib. MySQL is 6.
	zlibCompressionLevel = 6
)

func (p *packetIO) SetCompressionAlgorithm(algorithm CompressAlgorithm, zstdLevel int) error {
	switch algorithm {
	case CompressionZlib, CompressionZstd:
		p.readWriter = newCompressedReadWriter(p.readWriter, algorithm, zstdLevel, p.logger)
	case CompressionNone:
	default:
		return errors.Wrapf(mysql.ErrMalformPacket, "Unknown compression algorithm %d", algorithm)
	}
	return nil
}

var _ packetReadWriter = (*compressedReadWriter)(nil)

type compressedReadWriter struct {
	packetReadWriter
	readBuffer  bytes.Buffer
	writeBuffer bytes.Buffer
	algorithm   CompressAlgorithm
	logger      *zap.Logger
	rwStatus    rwStatus
	zstdLevel   zstd.EncoderLevel
	header      []byte
	sequence    uint8
}

func newCompressedReadWriter(rw packetReadWriter, algorithm CompressAlgorithm, zstdLevel int, logger *zap.Logger) *compressedReadWriter {
	return &compressedReadWriter{
		packetReadWriter: rw,
		algorithm:        algorithm,
		zstdLevel:        zstd.EncoderLevelFromZstd(zstdLevel),
		logger:           logger,
		rwStatus:         rwNone,
		header:           make([]byte, 7),
	}
}

func (crw *compressedReadWriter) ResetSequence() {
	crw.packetReadWriter.ResetSequence()
	// Reset the compressed sequence before the next command.
	// Sequence wraps around once it hits 0xFF, so we need ResetSequence() to know that it's reset instead of overflow.
	crw.sequence = 0
	crw.rwStatus = rwNone
}

// BeginRW implements packetReadWriter.BeginRW.
// Uncompressed sequence of MySQL doesn't follow the spec: it's set to the compressed sequence when
// the client/server begins reading or writing.
func (crw *compressedReadWriter) BeginRW(status rwStatus) {
	if crw.rwStatus != status {
		crw.packetReadWriter.SetSequence(crw.sequence)
		crw.rwStatus = status
	}
}

func (crw *compressedReadWriter) Read(p []byte) (n int, err error) {
	// Read from the connection to fill the buffer if the buffer is empty.
	if crw.readBuffer.Len() == 0 {
		if err = crw.readFromConn(); err != nil {
			return
		}
	}
	n, err = crw.readBuffer.Read(p)
	// Trade off between memory and efficiency.
	if n == len(p) && crw.readBuffer.Len() == 0 && crw.readBuffer.Cap() > DefaultConnBufferSize {
		crw.readBuffer = bytes.Buffer{}
	}
	return
}

func (crw *compressedReadWriter) ReadFrom(r io.Reader) (n int64, err error) {
	// TODO: copy compressed data directly.
	buf := make([]byte, DefaultConnBufferSize)
	nn := 0
	for {
		nn, err = r.Read(buf)
		if (err == nil || err == io.EOF) && nn > 0 {
			_, err = crw.Write(buf[:nn])
			n += int64(nn)
		}
		if err == io.EOF {
			return n, nil
		} else if err != nil {
			return n, err
		}
	}
}

// Read and uncompress the data into readBuffer.
// The format of the protocol: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_compression_packet.html
func (crw *compressedReadWriter) readFromConn() error {
	var err error
	if err = ReadFull(crw.packetReadWriter, crw.header); err != nil {
		return err
	}
	compressedSequence := crw.header[3]
	if compressedSequence != crw.sequence {
		return errors.WithStack(errors.Wrapf(ErrInvalidSequence,
			"invalid compressed sequence, expected %d, actual %d", crw.sequence, compressedSequence))
	}
	crw.sequence++
	compressedLength := int(uint32(crw.header[0]) | uint32(crw.header[1])<<8 | uint32(crw.header[2])<<16)
	uncompressedLength := int(uint32(crw.header[4]) | uint32(crw.header[5])<<8 | uint32(crw.header[6])<<16)

	if uncompressedLength == 0 {
		// If the data is uncompressed, the uncompressed length is 0 and compressed length is the data length
		// after the compressed header.
		crw.readBuffer.Grow(compressedLength)
		if _, err = io.CopyN(&crw.readBuffer, crw.packetReadWriter, int64(compressedLength)); err != nil {
			return errors.WithStack(err)
		}
	} else {
		// If the data is compressed, the compressed length is the length of data after the compressed header and
		// the uncompressed length is the length of data after decompression.
		data := make([]byte, compressedLength)
		if err = ReadFull(crw.packetReadWriter, data); err != nil {
			return err
		}
		if err = crw.uncompress(data, uncompressedLength); err != nil {
			return err
		}
	}
	return nil
}

func (crw *compressedReadWriter) Write(data []byte) (n int, err error) {
	for {
		remainingLen := maxCompressedSize - crw.writeBuffer.Len()
		if len(data) <= remainingLen {
			written, err := crw.writeBuffer.Write(data)
			if err != nil {
				return n, err
			}
			return n + written, nil
		}
		written, err := crw.writeBuffer.Write(data[:remainingLen])
		if err != nil {
			return n, err
		}
		n += written
		data = data[remainingLen:]
		if err = crw.Flush(); err != nil {
			return n, err
		}
	}
}

func (crw *compressedReadWriter) Flush() error {
	var err error
	data := crw.writeBuffer.Bytes()
	if len(data) == 0 {
		return nil
	}
	// Trade off between memory and efficiency.
	if crw.writeBuffer.Cap() > DefaultConnBufferSize {
		crw.writeBuffer = bytes.Buffer{}
	} else {
		crw.writeBuffer.Reset()
	}

	// If the data is uncompressed, the uncompressed length is 0 and compressed length is the data length
	// after the compressed header.
	uncompressedLength := 0
	compressedLength := len(data)
	if len(data) >= minCompressSize {
		// If the data is compressed, the compressed length is the length of data after the compressed header and
		// the uncompressed length is the length of data after decompression.
		uncompressedLength = len(data)
		if data, err = crw.compress(data); err != nil {
			return err
		}
		compressedLength = len(data)
	}

	crw.header[0] = byte(compressedLength)
	crw.header[1] = byte(compressedLength >> 8)
	crw.header[2] = byte(compressedLength >> 16)
	crw.header[3] = crw.sequence
	crw.header[4] = byte(uncompressedLength)
	crw.header[5] = byte(uncompressedLength >> 8)
	crw.header[6] = byte(uncompressedLength >> 16)
	crw.sequence++
	if _, err = crw.packetReadWriter.Write(crw.header[:]); err != nil {
		return errors.WithStack(err)
	}
	if _, err = crw.packetReadWriter.Write(data); err != nil {
		return errors.WithStack(err)
	}
	return crw.packetReadWriter.Flush()
}

// DirectWrite won't be used.
func (crw *compressedReadWriter) DirectWrite(data []byte) (n int, err error) {
	if n, err = crw.Write(data); err != nil {
		return
	}
	return n, crw.Flush()
}

// Peek won't be used.
// Notice: the peeked data may be discarded if an error is returned.
func (crw *compressedReadWriter) Peek(n int) (data []byte, err error) {
	for crw.readBuffer.Len() < n {
		if err = crw.readFromConn(); err != nil {
			return
		}
	}
	return crw.readBuffer.Bytes()[:n], nil
}

// Discard won't be used.
func (crw *compressedReadWriter) Discard(n int) (d int, err error) {
	for crw.readBuffer.Len() < n {
		if err = crw.readFromConn(); err != nil {
			return
		}
	}
	crw.readBuffer.Next(n)
	return n, err
}

// DataDog/zstd is much faster but it's not good at cross-platform.
// https://github.com/go-mysql-org/go-mysql/issues/799
func (crw *compressedReadWriter) compress(data []byte) ([]byte, error) {
	var err error
	var compressedPacket bytes.Buffer
	var compressWriter io.WriteCloser
	switch crw.algorithm {
	case CompressionZlib:
		compressWriter, err = zlib.NewWriterLevel(&compressedPacket, zlibCompressionLevel)
	case CompressionZstd:
		compressWriter, err = zstd.NewWriter(&compressedPacket, zstd.WithEncoderLevel(crw.zstdLevel))
	}
	if err != nil {
		return nil, errors.WithStack(errors.Wrap(err, mysql.ErrMalformPacket))
	}
	if _, err = compressWriter.Write(data); err != nil {
		return nil, errors.WithStack(err)
	}
	if err = compressWriter.Close(); err != nil {
		return nil, errors.WithStack(err)
	}
	return compressedPacket.Bytes(), nil
}

func (crw *compressedReadWriter) uncompress(data []byte, uncompressedLength int) error {
	var err error
	var compressedReader io.ReadCloser
	switch crw.algorithm {
	case CompressionZlib:
		if compressedReader, err = zlib.NewReader(bytes.NewReader(data)); err != nil {
			return errors.WithStack(errors.Wrap(err, mysql.ErrMalformPacket))
		}
	case CompressionZstd:
		var decoder *zstd.Decoder
		if decoder, err = zstd.NewReader(bytes.NewReader(data)); err != nil {
			return errors.WithStack(errors.Wrap(err, mysql.ErrMalformPacket))
		}
		compressedReader = decoder.IOReadCloser()
	}
	crw.readBuffer.Grow(uncompressedLength)
	if _, err = io.CopyN(&crw.readBuffer, compressedReader, int64(uncompressedLength)); err != nil {
		return errors.WithStack(err)
	}
	if err = compressedReader.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
