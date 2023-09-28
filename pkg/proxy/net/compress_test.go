// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// Test read/write with zlib compression.
func TestCompressZlib(t *testing.T) {
	sizes := []int{minCompressSize - 1, 1024, maxCompressedSize, maxCompressedSize + 1, maxCompressedSize * 2}
	lg, _ := logger.CreateLoggerForTest(t)
	testkit.TestTCPConn(t,
		func(t *testing.T, c net.Conn) {
			crw := newCompressedReadWriter(newBasicReadWriter(c), CompressionZlib, 0, lg)
			written := crw.OutBytes()
			for _, size := range sizes {
				fillAndWrite(t, crw, 'a', size)
				require.NoError(t, crw.Flush())
				// Check compressed bytes.
				outBytes := crw.OutBytes()
				checkByteNum(t, outBytes-written, size)
				written = outBytes
			}
		},
		func(t *testing.T, c net.Conn) {
			crw := newCompressedReadWriter(newBasicReadWriter(c), CompressionZlib, 0, lg)
			for _, size := range sizes {
				readAndCheck(t, crw, 'a', size)
			}
		}, 1)
}

// Test read/write with zstd compression.
func TestCompressZstd(t *testing.T) {
	sizes := []int{minCompressSize - 1, 1024, maxCompressedSize, maxCompressedSize + 1, maxCompressedSize * 2}
	levels := []int{1, 3, 9, 22}
	lg, _ := logger.CreateLoggerForTest(t)
	for _, level := range levels {
		testkit.TestTCPConn(t,
			func(t *testing.T, c net.Conn) {
				crw := newCompressedReadWriter(newBasicReadWriter(c), CompressionZstd, level, lg)
				written := crw.OutBytes()
				for _, size := range sizes {
					fillAndWrite(t, crw, 'a', size)
					require.NoError(t, crw.Flush())
					// Check compressed bytes.
					outBytes := crw.OutBytes()
					checkByteNum(t, outBytes-written, size)
					written = outBytes
				}
			},
			func(t *testing.T, c net.Conn) {
				crw := newCompressedReadWriter(newBasicReadWriter(c), CompressionZstd, level, lg)
				for _, size := range sizes {
					readAndCheck(t, crw, 'a', size)
				}
			}, 1)
	}
}

// Test that multiple packets are merged into one compressed packet.
func TestCompressMergePkt(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	sizes := make([]int, 50)
	for i := range sizes {
		sizes[i] = int(rand.Int31n(maxCompressedSize / 2))
	}
	testkit.TestTCPConn(t,
		func(t *testing.T, c net.Conn) {
			crw := newCompressedReadWriter(newBasicReadWriter(c), CompressionZlib, 0, lg)
			written := 0
			for i, size := range sizes {
				fillAndWrite(t, crw, 'a'+byte(i), size)
				// Check that data is buffered until reaching maxCompressedSize.
				written += size
				require.Equal(t, written%maxCompressedSize, crw.writeBuffer.Len())
			}
			require.NoError(t, crw.Flush())
		},
		func(t *testing.T, c net.Conn) {
			crw := newCompressedReadWriter(newBasicReadWriter(c), CompressionZlib, 0, lg)
			for i, size := range sizes {
				readAndCheck(t, crw, 'a'+byte(i), size)
			}
		}, 1)
}

// Test that DirectWrite(), Peek(), and Discard() work well.
func TestCompressPeekDiscard(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	size := 1000
	testkit.TestTCPConn(t,
		func(t *testing.T, c net.Conn) {
			crw := newCompressedReadWriter(newBasicReadWriter(c), CompressionZlib, 0, lg)
			data := fillData('a', size)
			_, err := crw.DirectWrite(data)
			require.NoError(t, err)

			data = fillData('b', size)
			_, err = crw.DirectWrite(data)
			require.NoError(t, err)
		},
		func(t *testing.T, c net.Conn) {
			crw := newCompressedReadWriter(newBasicReadWriter(c), CompressionZlib, 0, lg)
			peek, err := crw.Peek(10)
			require.NoError(t, err)
			checkData(t, peek, 'a')
			readAndCheck(t, crw, 'a', size)

			_, err = crw.Discard(100)
			require.NoError(t, err)
			readAndCheck(t, crw, 'b', size-100)
		}, 1)
}

// Test that the uncompressed sequence is correct.
func TestCompressSequence(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	testkit.TestTCPConn(t,
		func(t *testing.T, c net.Conn) {
			crw := newCompressedReadWriter(newBasicReadWriter(c), CompressionZlib, 0, lg)
			fillAndWrite(t, crw, 'a', 100)
			fillAndWrite(t, crw, 'a', 100)
			require.NoError(t, crw.Flush())
			require.Equal(t, uint8(2), crw.Sequence())
			// uncompressed sequence = compressed sequence
			readAndCheck(t, crw, 'a', 100)
			require.Equal(t, uint8(2), crw.Sequence())
			readAndCheck(t, crw, 'a', 100)
			require.Equal(t, uint8(3), crw.Sequence())
			// uncompressed sequence = compressed sequence
			fillAndWrite(t, crw, 'a', maxCompressedSize+1)
			require.NoError(t, crw.Flush())
			require.Equal(t, uint8(3), crw.Sequence())
			// uncompressed sequence = compressed sequence
			readAndCheck(t, crw, 'a', maxCompressedSize+1)
			require.Equal(t, uint8(5), crw.Sequence())
			// flush empty buffer won't increase sequence
			require.NoError(t, crw.Flush())
			require.NoError(t, crw.Flush())
			fillAndWrite(t, crw, 'a', 100)
			require.NoError(t, crw.Flush())
		},
		func(t *testing.T, c net.Conn) {
			crw := newCompressedReadWriter(newBasicReadWriter(c), CompressionZlib, 0, lg)
			readAndCheck(t, crw, 'a', 100)
			readAndCheck(t, crw, 'a', 100)
			require.Equal(t, uint8(2), crw.Sequence())
			// uncompressed sequence = compressed sequence
			fillAndWrite(t, crw, 'a', 100)
			require.Equal(t, uint8(2), crw.Sequence())
			fillAndWrite(t, crw, 'a', 100)
			require.Equal(t, uint8(3), crw.Sequence())
			require.NoError(t, crw.Flush())
			// uncompressed sequence = compressed sequence
			readAndCheck(t, crw, 'a', maxCompressedSize+1)
			require.Equal(t, uint8(3), crw.Sequence())
			// uncompressed sequence = compressed sequence
			fillAndWrite(t, crw, 'a', maxCompressedSize+1)
			require.NoError(t, crw.Flush())
			require.Equal(t, uint8(5), crw.Sequence())
			// flush empty buffer won't increase sequence
			readAndCheck(t, crw, 'a', 100)
		}, 1)
}

// Test that the compressed header is correctly filled.
func TestCompressHeader(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	sizes := []int{minCompressSize - 1, maxCompressedSize, maxCompressedSize + 1}
	testkit.TestTCPConn(t,
		func(t *testing.T, c net.Conn) {
			crw := newCompressedReadWriter(newBasicReadWriter(c), CompressionZlib, 0, lg)
			for i, size := range sizes {
				fillAndWrite(t, crw, 'a'+byte(i), size)
				require.NoError(t, crw.Flush())
			}
		},
		func(t *testing.T, c net.Conn) {
			brw := newBasicReadWriter(c)
			crw := newCompressedReadWriter(brw, CompressionZlib, 0, lg)
			for i, size := range sizes {
				header, err := brw.Peek(7)
				require.NoError(t, err)
				compressedLength := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
				uncompressedLength := int(uint32(header[4]) | uint32(header[5])<<8 | uint32(header[6])<<16)
				if size < minCompressSize {
					require.Equal(t, size, compressedLength)
					require.Equal(t, 0, uncompressedLength)
				} else if size <= maxCompressedSize {
					require.Greater(t, compressedLength, 0)
					require.Less(t, compressedLength, size)
					require.Equal(t, size, uncompressedLength)
				} else {
					require.Greater(t, compressedLength, 0)
					require.Less(t, compressedLength, maxCompressedSize)
					require.Equal(t, maxCompressedSize, uncompressedLength)
				}
				readAndCheck(t, crw, 'a'+byte(i), size)
			}
		}, 1)
}

func fillAndWrite(t *testing.T, crw *compressedReadWriter, b byte, length int) {
	data := fillData(b, length)
	_, err := crw.Write(data)
	require.NoError(t, err)
	crw.SetSequence(crw.Sequence() + 1)
}

func fillData(b byte, length int) []byte {
	data := make([]byte, length)
	for i := range data {
		data[i] = b
	}
	return data
}

func readAndCheck(t *testing.T, crw *compressedReadWriter, b byte, length int) {
	data := make([]byte, length)
	_, err := io.ReadFull(crw, data)
	require.NoError(t, err)
	checkData(t, data, b)
	crw.SetSequence(crw.Sequence() + 1)
}

func checkData(t *testing.T, data []byte, b byte) {
	for i := range data {
		if data[i] != b {
			require.Fail(t, fmt.Sprintf("expected %c, but got %c", b, data[i]))
		}
	}
}

func checkByteNum(t *testing.T, diff uint64, size int) {
	if size < minCompressSize {
		require.Equal(t, uint64(size+7), diff)
	} else {
		require.Greater(t, diff, uint64(0))
		require.Less(t, diff, uint64(size+7))
	}
}
