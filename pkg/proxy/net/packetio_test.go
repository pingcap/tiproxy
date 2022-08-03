// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package net

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/pingcap/TiProxy/pkg/util/security"
	"github.com/pingcap/TiProxy/pkg/util/waitgroup"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

func testPipeConn(t *testing.T, a func(*testing.T, *PacketIO), b func(*testing.T, *PacketIO), loop int) {
	var wg waitgroup.WaitGroup
	client, server := net.Pipe()
	cli, srv := NewPacketIO(client), NewPacketIO(server)
	for i := 0; i < loop; i++ {
		wg.Run(func() {
			a(t, cli)
			require.NoError(t, cli.Close())
		})
		wg.Run(func() {
			b(t, srv)
			require.NoError(t, srv.Close())
		})
		wg.Wait()
	}
}

func testTCPConn(t *testing.T, a func(*testing.T, *PacketIO), b func(*testing.T, *PacketIO), loop int) {
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, listener.Close())
	}()
	var wg waitgroup.WaitGroup
	for i := 0; i < loop; i++ {
		wg.Run(func() {
			cli, err := net.Dial("tcp", listener.Addr().String())
			require.NoError(t, err)
			cliIO := NewPacketIO(cli)
			a(t, cliIO)
			require.NoError(t, cliIO.Close())
		})
		wg.Run(func() {
			srv, err := listener.Accept()
			require.NoError(t, err)
			srvIO := NewPacketIO(srv)
			b(t, srvIO)
			require.NoError(t, srvIO.Close())
		})
		wg.Wait()
	}
}

func TestPacketIO(t *testing.T) {
	expectMsg := []byte("test")
	testPipeConn(t,
		func(t *testing.T, cli *PacketIO) {
			var err error

			// send anything
			require.NoError(t, cli.WritePacket(expectMsg, true))

			// send more than max payload
			require.NoError(t, cli.WritePacket(make([]byte, mysql.MaxPayloadLen+212), true))
			require.NoError(t, cli.WritePacket(make([]byte, mysql.MaxPayloadLen), true))
			require.NoError(t, cli.WritePacket(make([]byte, mysql.MaxPayloadLen*2), true))

			// skip handshake
			_, err = cli.ReadPacket()
			require.NoError(t, err)

			// send correct and wrong capability flags
			var hdr [4]byte
			binary.LittleEndian.PutUint16(hdr[:], uint16(mysql.ClientSSL))
			err = cli.WritePacket(hdr[:], true)
			require.NoError(t, err)

			binary.LittleEndian.PutUint16(hdr[:], 0)
			err = cli.WritePacket(hdr[:], true)
			require.NoError(t, err)
		},
		func(t *testing.T, srv *PacketIO) {
			var salt [8]byte
			var msg []byte
			var err error

			// receive "test"
			msg, err = srv.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, msg, expectMsg)

			// receive more than max payload
			_, err = srv.ReadPacket()
			require.NoError(t, err)
			_, err = srv.ReadPacket()
			require.NoError(t, err)
			_, err = srv.ReadPacket()
			require.NoError(t, err)

			// send handshake
			require.NoError(t, srv.WriteInitialHandshake(0, salt[:], mysql.AuthNativePassword))

			// expect correct and wrong capability flags
			_, err = srv.ReadSSLRequest()
			require.NoError(t, err)
			_, err = srv.ReadSSLRequest()
			require.ErrorIs(t, err, ErrExpectSSLRequest)
		},
		1,
	)
}

func TestTLS(t *testing.T) {
	stls, ctls, err := security.CreateTLSConfigForTest()
	require.NoError(t, err)
	message := []byte("hello wolrd")
	testTCPConn(t,
		func(t *testing.T, cli *PacketIO) {
			data, err := cli.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, message, data)
			err = cli.WritePacket(message, true)
			require.NoError(t, err)

			require.NoError(t, cli.UpgradeToClientTLS(ctls))

			err = cli.WritePacket(message, true)
			require.NoError(t, err)
			data, err = cli.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, message, data)
		},
		func(t *testing.T, srv *PacketIO) {
			err = srv.WritePacket(message, true)
			require.NoError(t, err)
			data, err := srv.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, message, data)

			_, err = srv.UpgradeToServerTLS(stls)
			require.NoError(t, err)

			data, err = srv.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, message, data)
			err = srv.WritePacket(message, true)
			require.NoError(t, err)
		},
		500, // unable to reproduce stably, loop 500 times
	)
}
