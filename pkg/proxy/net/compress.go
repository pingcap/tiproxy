// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"bytes"
	"compress/zlib"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/tiproxy/lib/util/errors"
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
	// maxCompressedSize is the max size for compressed data.
	// MySQL starts with `net_buffer_length` (default 16384) and larger packets after that.
	// The length itself must fit in the 3 byte field in the header.
	// Can't be bigger then the max value for `net_buffer_length` (1048576)
	maxCompressedSize = 1024 * 1024
	// minCompressSize is the min size for compressed data.
	// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_compression_packet.html
	// suggests a MIN_COMPRESS_LENGTH of 50.
	minCompressSize = 50
)

func (p *PacketIO) SetCompressionAlgorithm(algorithm CompressAlgorithm, zstdLevel int) error {
	switch algorithm {
	case CompressionZlib, CompressionZstd:
	case CompressionNone:
		return nil
	default:
		return errors.Errorf("Unknown compression algorithm %d", algorithm)
	}
	rw, err := newCompressedReadWriter(p.readWriter, algorithm, zstdLevel)
	if err != nil {
		return err
	}
	p.readWriter = rw
	return nil
}

var _ packetReadWriter = (*compressedReadWriter)(nil)

type compressedReadWriter struct {
	packetReadWriter
	compressedReader io.ReadCloser
	buf              *bytes.Buffer
	algorithm        CompressAlgorithm
	zstdLevel        zstd.EncoderLevel
	sequence         uint8
}

func newCompressedReadWriter(rw packetReadWriter, algorithm CompressAlgorithm, zstdLevel int) (*compressedReadWriter, error) {
	if err := rw.Flush(); err != nil {
		return nil, err
	}
	return &compressedReadWriter{
		packetReadWriter: rw,
		buf:              new(bytes.Buffer),
		algorithm:        algorithm,
		zstdLevel:        zstd.EncoderLevelFromZstd(zstdLevel),
	}, nil
}

func (crw *compressedReadWriter) reset() {
	crw.sequence = 0
}

func (crw *compressedReadWriter) afterRead() {
	if crw.compressedReader != nil {
		_ = crw.compressedReader.Close()
		crw.compressedReader = nil
	}
}

func (crw *compressedReadWriter) Read(p []byte) (n int, err error) {
	if crw.compressedReader == nil {
		var compressedHeader [7]byte
		if _, err := io.ReadFull(crw.packetReadWriter, compressedHeader[:]); err != nil {
			return 0, errors.WithStack(err)
		}
		compressedSequence := compressedHeader[3]
		if compressedSequence != crw.sequence {
			return 0, ErrInvalidSequence.GenWithStack(
				"invalid compressed sequence, expected %d, actual %d", crw.sequence, compressedSequence)
		}
		crw.sequence++
		uncompressedLength := int(uint32(compressedHeader[4]) | uint32(compressedHeader[5])<<8 | uint32(compressedHeader[6])<<16)
		if uncompressedLength <= 0 {
			return crw.packetReadWriter.Read(p)
		}

		switch crw.algorithm {
		case CompressionZlib:
			crw.compressedReader, err = zlib.NewReader(crw.packetReadWriter)
		case CompressionZstd:
			var zstdReader *zstd.Decoder
			if zstdReader, err = zstd.NewReader(crw.packetReadWriter, zstd.WithDecoderConcurrency(1)); err == nil {
				crw.compressedReader = zstdReader.IOReadCloser()
			}
		}
		if err != nil {
			return 0, errors.WithStack(err)
		}
	}
	return crw.compressedReader.Read(p)
}

func (crw *compressedReadWriter) Write(data []byte) (n int, err error) {
	for {
		remainingLen := maxCompressedSize - crw.buf.Len()
		if len(data) <= remainingLen {
			written, err := crw.buf.Write(data)
			if err != nil {
				return 0, err
			}
			return n + written, nil
		}
		written, err := crw.buf.Write(data[:remainingLen])
		if err != nil {
			return 0, err
		}
		n += written
		data = data[remainingLen:]
		if err = crw.flush(); err != nil {
			return 0, err
		}
	}
}

func (crw *compressedReadWriter) flush() error {
	var payload, compressedPacket bytes.Buffer
	var w io.WriteCloser
	var err error

	data := crw.buf.Bytes()
	crw.buf.Reset()

	switch crw.algorithm {
	case CompressionZlib:
		w, err = zlib.NewWriterLevel(&payload, zlib.HuffmanOnly)
	case CompressionZstd:
		w, err = zstd.NewWriter(&payload, zstd.WithEncoderLevel(crw.zstdLevel))
	}
	if err != nil {
		return errors.WithStack(err)
	}

	uncompressedLength := 0
	compressedLength := len(data)
	compressedHeader := make([]byte, 7)

	if len(data) > minCompressSize {
		uncompressedLength = len(data)
		if _, err = w.Write(data); err != nil {
			return errors.WithStack(err)
		}
		if err = w.Close(); err != nil {
			return errors.WithStack(err)
		}
		compressedLength = len(payload.Bytes())
	}
	compressedHeader[0] = byte(compressedLength)
	compressedHeader[1] = byte(compressedLength >> 8)
	compressedHeader[2] = byte(compressedLength >> 16)
	compressedHeader[3] = crw.sequence
	compressedHeader[4] = byte(uncompressedLength)
	compressedHeader[5] = byte(uncompressedLength >> 8)
	compressedHeader[6] = byte(uncompressedLength >> 16)
	if _, err = compressedPacket.Write(compressedHeader); err != nil {
		return errors.WithStack(err)
	}
	crw.sequence++

	if len(data) > minCompressSize {
		_, err = compressedPacket.Write(payload.Bytes())
	} else {
		_, err = compressedPacket.Write(data)
	}
	if err != nil {
		return errors.WithStack(err)
	}
	if err = w.Close(); err != nil {
		return errors.WithStack(err)
	}
	if _, err = crw.packetReadWriter.Write(compressedPacket.Bytes()); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
