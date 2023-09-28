// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"bytes"
	"compress/zlib"
	"io"

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

type rwStatus int

const (
	rwNone rwStatus = iota
	rwRead
	rwWrite
)

const (
	// maxCompressedSize is the max uncompressed data size for a compressed packet.
	// Packets bigger than maxCompressedSize will be split into multiple compressed packets.
	// MySQL is 16K for the first packet and the rest for the second, MySQL Connector/J is 16M.
	// The length itself must fit in the 3 byte field in the header.
	maxCompressedSize = 1<<24 - 1
	// minCompressSize is the min uncompressed data size for compressed data.
	// Packets smaller than minCompressSize won't be compressed.
	// MySQL and MySQL Connector/J are both 50.
	minCompressSize = 50
)

func (p *PacketIO) SetCompressionAlgorithm(algorithm CompressAlgorithm, zstdLevel int) error {
	switch algorithm {
	case CompressionZlib, CompressionZstd:
		p.readWriter = newCompressedReadWriter(p.readWriter, algorithm, zstdLevel, p.logger)
	case CompressionNone:
	default:
		return errors.Errorf("Unknown compression algorithm %d", algorithm)
	}
	return nil
}

var _ packetReadWriter = (*compressedReadWriter)(nil)

type compressedReadWriter struct {
	packetReadWriter
	readBuffer  []byte
	writeBuffer bytes.Buffer
	algorithm   CompressAlgorithm
	logger      *zap.Logger
	rwStatus    rwStatus
	zstdLevel   int
	sequence    uint8
}

func newCompressedReadWriter(rw packetReadWriter, algorithm CompressAlgorithm, zstdLevel int, logger *zap.Logger) *compressedReadWriter {
	return &compressedReadWriter{
		packetReadWriter: rw,
		algorithm:        algorithm,
		zstdLevel:        zstdLevel,
		logger:           logger,
		rwStatus:         rwNone,
	}
}

func (crw *compressedReadWriter) SetSequence(sequence uint8) {
	crw.packetReadWriter.SetSequence(sequence)
	// Reset the compressed sequence before the next command.
	if sequence == 0 {
		crw.sequence = 0
		crw.rwStatus = rwNone
	}
}

// Uncompressed sequence of MySQL doesn't follow the spec: it's set to the compressed sequence when
// the client/server begins reading or writing.
func (crw *compressedReadWriter) beginRW(status rwStatus) {
	if crw.rwStatus != status {
		crw.packetReadWriter.SetSequence(crw.sequence)
		crw.rwStatus = status
	}
}

func (crw *compressedReadWriter) Read(p []byte) (n int, err error) {
	crw.beginRW(rwRead)
	// Read from the connection to fill the buffer if the buffer is empty.
	if len(crw.readBuffer) == 0 {
		if err = crw.readFromConn(); err != nil {
			return
		}
	}
	n = copy(p, crw.readBuffer)
	if n == len(crw.readBuffer) {
		// Free the buffer. Too many idle connections may hold too much unnecessary memory.
		crw.readBuffer = nil
	} else {
		crw.readBuffer = crw.readBuffer[n:]
	}
	return
}

// Read and uncompress the data into readBuffer.
// The format of the protocol: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_compression_packet.html
func (crw *compressedReadWriter) readFromConn() error {
	var err error
	var header [7]byte
	if _, err = io.ReadFull(crw.packetReadWriter, header[:]); err != nil {
		return err
	}
	compressedSequence := header[3]
	if compressedSequence != crw.sequence {
		return ErrInvalidSequence.GenWithStack(
			"invalid compressed sequence, expected %d, actual %d", crw.sequence, compressedSequence)
	}
	crw.sequence++
	compressedLength := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	uncompressedLength := int(uint32(header[4]) | uint32(header[5])<<8 | uint32(header[6])<<16)

	if uncompressedLength == 0 {
		// If the data is uncompressed, the uncompressed length is 0 and compressed length is the data length
		// after the compressed header.
		crw.readBuffer = make([]byte, compressedLength)
		if _, err = io.ReadFull(crw.packetReadWriter, crw.readBuffer); err != nil {
			return err
		}
	} else {
		// If the data is compressed, the compressed length is the length of data after the compressed header and
		// the uncompressed length is the length of data after decompression.
		data := make([]byte, compressedLength)
		if _, err = io.ReadFull(crw.packetReadWriter, data); err != nil {
			return err
		}
		if crw.readBuffer, err = crw.uncompress(data, uncompressedLength); err != nil {
			return err
		}
	}
	return nil
}

func (crw *compressedReadWriter) Write(data []byte) (n int, err error) {
	crw.beginRW(rwWrite)
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
	crw.writeBuffer.Reset()

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

	var compressedHeader [7]byte
	compressedHeader[0] = byte(compressedLength)
	compressedHeader[1] = byte(compressedLength >> 8)
	compressedHeader[2] = byte(compressedLength >> 16)
	compressedHeader[3] = crw.sequence
	compressedHeader[4] = byte(uncompressedLength)
	compressedHeader[5] = byte(uncompressedLength >> 8)
	compressedHeader[6] = byte(uncompressedLength >> 16)
	crw.sequence++
	if _, err = crw.packetReadWriter.Write(compressedHeader[:]); err != nil {
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
	crw.beginRW(rwRead)
	var readBuffer []byte
	for len(readBuffer) < n {
		if len(crw.readBuffer) == 0 {
			if err = crw.readFromConn(); err != nil {
				return
			}
		}
		readBuffer = append(readBuffer, crw.readBuffer...)
		crw.readBuffer = nil
	}
	data = make([]byte, 0, n)
	copy(data, readBuffer)
	crw.readBuffer = readBuffer
	return
}

// Discard won't be used.
func (crw *compressedReadWriter) Discard(n int) (d int, err error) {
	crw.beginRW(rwRead)
	for left := n; left > 0; {
		if len(crw.readBuffer) == 0 {
			if err = crw.readFromConn(); err != nil {
				return
			}
		}
		if left >= len(crw.readBuffer) {
			left -= len(crw.readBuffer)
			crw.readBuffer = nil
			d += len(crw.readBuffer)
		} else {
			left = 0
			crw.readBuffer = crw.readBuffer[left:]
			d += left
		}
	}
	return
}

// DataDog/zstd is much faster but it's not good at cross-platform.
// https://github.com/go-mysql-org/go-mysql/issues/799
func (crw *compressedReadWriter) compress(data []byte) ([]byte, error) {
	var err error
	var compressedPacket bytes.Buffer
	var compressWriter io.WriteCloser
	switch crw.algorithm {
	case CompressionZlib:
		compressWriter, err = zlib.NewWriterLevel(&compressedPacket, zlib.DefaultCompression)
	case CompressionZstd:
		compressWriter, err = zstd.NewWriter(&compressedPacket, zstd.WithEncoderLevel(zstd.EncoderLevel(crw.zstdLevel)))
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if _, err = compressWriter.Write(data); err != nil {
		return nil, errors.WithStack(err)
	}
	if err = compressWriter.Close(); err != nil {
		return nil, errors.WithStack(err)
	}
	return compressedPacket.Bytes(), nil
}

func (crw *compressedReadWriter) uncompress(data []byte, uncompressedLength int) ([]byte, error) {
	var err error
	var compressedReader io.ReadCloser
	switch crw.algorithm {
	case CompressionZlib:
		if compressedReader, err = zlib.NewReader(bytes.NewReader(data)); err != nil {
			return nil, errors.WithStack(err)
		}
	case CompressionZstd:
		var decoder *zstd.Decoder
		if decoder, err = zstd.NewReader(bytes.NewReader(data)); err != nil {
			return nil, errors.WithStack(err)
		}
		compressedReader = decoder.IOReadCloser()
	}
	uncompressed := make([]byte, uncompressedLength)
	if _, err = io.ReadFull(compressedReader, uncompressed); err != nil {
		return nil, errors.WithStack(err)
	}
	if err = compressedReader.Close(); err != nil {
		return nil, errors.WithStack(err)
	}
	return uncompressed, nil
}
