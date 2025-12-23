// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package bufio

import (
	"bytes"
	"io"
	"net"
	"testing"

	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	"github.com/stretchr/testify/require"
)

func TestReadFrom(t *testing.T) {
	// The reader breaks and ReadFrom returns ErrReadFail.
	var wg waitgroup.WaitGroup
	p1, p2 := net.Pipe()
	writer := bytes.NewBuffer(make([]byte, 10))
	bufWriter := NewWriter(writer)
	wg.Run(func() {
		_, err := p1.Write([]byte("h"))
		require.NoError(t, err)
		require.NoError(t, p2.Close())
		require.NoError(t, p1.Close())
	}, nil)
	wg.Run(func() {
		n, err := bufWriter.ReadFrom(io.LimitReader(p2, 1))
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
		_, err = bufWriter.ReadFrom(p2)
		require.ErrorIs(t, err, ErrReadFail)
	}, nil)
	wg.Wait()

	// The writer breaks and ReadFrom returns ErrWriteFail.
	p1, p2 = net.Pipe()
	bufWriter = NewWriterSize(p1, 2)
	reader := bytes.NewBuffer([]byte("he"))
	n, err := bufWriter.ReadFrom(io.LimitReader(reader, 1))
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	require.NoError(t, p2.Close())
	_, err = bufWriter.ReadFrom(io.LimitReader(reader, 1))
	require.ErrorIs(t, err, ErrWriteFail)
}
