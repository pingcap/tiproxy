// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"crypto/tls"
	"io"
	"net"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/security"
	"github.com/pingcap/tiproxy/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestTLSReadWrite(t *testing.T) {
	stls, ctls, err := security.CreateTLSConfigForTest()
	require.NoError(t, err)
	message := []byte("hello world")
	ch := make(chan []byte)
	testkit.TestTCPConn(t,
		func(t *testing.T, c net.Conn) {
			brw := newBasicReadWriter(c, DefaultConnBufferSize)
			conn := &tlsInternalConn{brw}
			tlsConn := tls.Client(conn, ctls)
			require.NoError(t, tlsConn.Handshake())
			trw := newTLSReadWriter(brw, tlsConn)
			// check tls connection state
			require.True(t, trw.TLSConnectionState().HandshakeComplete)
			// check out bytes
			outBytes := trw.OutBytes()
			// Wait before writing, otherwise the message is buffered during TLS in the other goroutine.
			ch <- message
			n, err := trw.Write(message)
			require.NoError(t, err)
			require.NoError(t, trw.Flush())
			require.Equal(t, len(message), n)
			require.Greater(t, trw.OutBytes(), outBytes+uint64(len(message)))
			// check direct write
			for i := 0; i < 2; i++ {
				n, err = trw.DirectWrite(message)
				require.NoError(t, err)
				require.Equal(t, len(message), n)
			}
		},
		func(t *testing.T, c net.Conn) {
			brw := newBasicReadWriter(c, DefaultConnBufferSize)
			conn := &tlsInternalConn{brw}
			tlsConn := tls.Server(conn, stls)
			require.NoError(t, tlsConn.Handshake())
			trw := newTLSReadWriter(brw, tlsConn)
			// check tls connection state
			require.True(t, trw.TLSConnectionState().HandshakeComplete)
			// check in bytes
			inBytes := trw.InBytes()
			message := <-ch
			data := make([]byte, len(message))
			n, err := io.ReadFull(trw, data)
			require.NoError(t, err)
			require.Equal(t, len(message), n)
			require.Equal(t, message, data)
			require.Greater(t, trw.InBytes(), inBytes+uint64(len(message)))
			// check peek
			peek, err := trw.Peek(1)
			require.NoError(t, err)
			require.Len(t, peek, 1)
			require.Equal(t, message[0], peek[0])
			data = make([]byte, len(message))
			n, err = io.ReadFull(trw, data)
			require.NoError(t, err)
			require.Equal(t, len(data), n)
			require.Equal(t, message, data)
			// check discard
			n, err = trw.Discard(1)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			data = make([]byte, len(message)-1)
			n, err = io.ReadFull(trw, data)
			require.NoError(t, err)
			require.Equal(t, len(data), n)
			require.Equal(t, message[1:], data)
		}, 1)
}
