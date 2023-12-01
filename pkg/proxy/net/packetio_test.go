// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/security"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func testPipeConn(t *testing.T, a func(*testing.T, *PacketIO), b func(*testing.T, *PacketIO), loop int) {
	lg, _ := logger.CreateLoggerForTest(t)
	testkit.TestPipeConn(t,
		func(t *testing.T, c net.Conn) {
			a(t, NewPacketIO(c, lg, DefaultConnBufferSize))
		},
		func(t *testing.T, c net.Conn) {
			b(t, NewPacketIO(c, lg, DefaultConnBufferSize))
		}, loop)
}

func testTCPConn(t *testing.T, a func(*testing.T, *PacketIO), b func(*testing.T, *PacketIO), loop int) {
	lg, _ := logger.CreateLoggerForTest(t)
	testkit.TestTCPConn(t,
		func(t *testing.T, c net.Conn) {
			cli := NewPacketIO(c, lg, DefaultConnBufferSize)
			a(t, cli)
			require.NoError(t, cli.Close())
		},
		func(t *testing.T, c net.Conn) {
			srv := NewPacketIO(c, lg, DefaultConnBufferSize)
			b(t, srv)
			require.NoError(t, srv.Close())
		}, loop)
}

func TestPacketIO(t *testing.T) {
	expectMsg := []byte("test")
	pktLengths := []int{0, MaxPayloadLen + 212, MaxPayloadLen, MaxPayloadLen * 2}
	testPipeConn(t,
		func(t *testing.T, cli *PacketIO) {
			var err error

			// send anything
			require.NoError(t, cli.WritePacket(expectMsg, true))

			outBytes := len(expectMsg) + 4
			for _, l := range pktLengths {
				require.NoError(t, cli.WritePacket(make([]byte, l), true))
				outBytes += l + (l/(MaxPayloadLen)+1)*4
				require.Equal(t, uint64(outBytes), cli.OutBytes())
			}

			// skip handshake
			_, err = cli.ReadPacket()
			require.NoError(t, err)

			// send correct and wrong capability flags
			var hdr [32]byte
			binary.LittleEndian.PutUint32(hdr[:], ClientSSL.Uint32())
			err = cli.WritePacket(hdr[:], true)
			require.NoError(t, err)

			binary.LittleEndian.PutUint16(hdr[:], 0)
			err = cli.WritePacket(hdr[:], true)
			require.NoError(t, err)
		},
		func(t *testing.T, srv *PacketIO) {
			var salt [40]byte
			var msg []byte
			var err error

			// receive "test"
			msg, err = srv.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, expectMsg, msg)

			inBytes := len(expectMsg) + 4
			for _, l := range pktLengths {
				msg, err = srv.ReadPacket()
				require.NoError(t, err)
				require.Equal(t, l, len(msg))
				inBytes += l + (l/(MaxPayloadLen)+1)*4
				require.Equal(t, uint64(inBytes), srv.InBytes())
			}

			// send handshake
			require.NoError(t, srv.WriteInitialHandshake(0, salt[:], AuthNativePassword, ServerVersion, 100))
			// salt should not be long enough
			require.ErrorIs(t, srv.WriteInitialHandshake(0, make([]byte, 4), AuthNativePassword, ServerVersion, 100), ErrSaltNotLongEnough)

			// expect correct and wrong capability flags
			_, isSSL, err := srv.ReadSSLRequestOrHandshakeResp()
			require.NoError(t, err)
			require.True(t, isSSL)
			_, isSSL, err = srv.ReadSSLRequestOrHandshakeResp()
			require.NoError(t, err)
			require.False(t, isSSL)
		},
		1,
	)
}

func TestTLS(t *testing.T) {
	stls, ctls, err := security.CreateTLSConfigForTest()
	require.NoError(t, err)
	message := []byte("hello world")
	testTCPConn(t,
		func(t *testing.T, cli *PacketIO) {
			data, err := cli.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, message, data)
			err = cli.WritePacket(message, true)
			require.NoError(t, err)

			require.NoError(t, cli.ClientTLSHandshake(ctls))

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

			_, err = srv.ServerTLSHandshake(stls)
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

func TestPacketIOClose(t *testing.T) {
	testTCPConn(t,
		func(t *testing.T, cli *PacketIO) {
			require.NoError(t, cli.Close())
			require.NoError(t, cli.Close())
			require.NoError(t, cli.GracefulClose())
			require.NotEqual(t, cli.LocalAddr(), "")
			require.NotEqual(t, cli.RemoteAddr(), "")
		},
		func(t *testing.T, srv *PacketIO) {
			require.NoError(t, srv.GracefulClose())
			require.NoError(t, srv.Close())
			require.NoError(t, srv.Close())
			require.NotEqual(t, srv.LocalAddr(), "")
			require.NotEqual(t, srv.RemoteAddr(), "")
		},
		1,
	)
}

func TestPeerActive(t *testing.T) {
	stls, ctls, err := security.CreateTLSConfigForTest()
	require.NoError(t, err)
	ch := make(chan struct{})
	testTCPConn(t,
		func(t *testing.T, cli *PacketIO) {
			// It's active at the beginning.
			require.True(t, cli.IsPeerActive())
			ch <- struct{}{} // let srv write packet
			// ReadPacket still reads the whole data after checking.
			ch <- struct{}{}
			require.True(t, cli.IsPeerActive())
			data, err := cli.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, "123", string(data))
			// IsPeerActive works after reading data.
			require.True(t, cli.IsPeerActive())
			// IsPeerActive works after writing data.
			require.NoError(t, cli.WritePacket([]byte("456"), true))
			require.True(t, cli.IsPeerActive())
			// upgrade to TLS and try again
			require.NoError(t, cli.ClientTLSHandshake(ctls))
			require.True(t, cli.IsPeerActive())
			data, err = cli.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, "123", string(data))
			require.True(t, cli.IsPeerActive())
			require.NoError(t, cli.WritePacket([]byte("456"), true))
			require.True(t, cli.IsPeerActive())
			// It's not active after the peer closes.
			ch <- struct{}{}
			ch <- struct{}{}
			require.False(t, cli.IsPeerActive())
		},
		func(t *testing.T, srv *PacketIO) {
			<-ch
			err := srv.WritePacket([]byte("123"), true)
			require.NoError(t, err)
			<-ch
			data, err := srv.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, "456", string(data))
			// upgrade to TLS and try again
			_, err = srv.ServerTLSHandshake(stls)
			require.NoError(t, err)
			err = srv.WritePacket([]byte("123"), true)
			require.NoError(t, err)
			data, err = srv.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, "456", string(data))
			<-ch
			require.NoError(t, srv.Close())
			<-ch
		},
		10,
	)
}

func TestKeepAlive(t *testing.T) {
	stls, ctls, err := security.CreateTLSConfigForTest()
	require.NoError(t, err)
	frontend, backendHealthy, backendUnhealthy := config.DefaultKeepAlive()
	backendUnhealthy.Timeout = 2 * time.Second
	backendUnhealthy.Idle = time.Second
	backendUnhealthy.Cnt = 1
	backendUnhealthy.Intvl = time.Second
	testTCPConn(t,
		func(t *testing.T, cli *PacketIO) {
			require.NoError(t, cli.SetKeepalive(frontend))
			require.NoError(t, cli.ClientTLSHandshake(ctls))
			time.Sleep(3 * time.Second)
			_, err := cli.ReadPacket()
			require.NoError(t, err)
			require.NoError(t, cli.WritePacket([]byte{0, 1, 2}, true))
		},
		func(t *testing.T, srv *PacketIO) {
			require.NoError(t, srv.SetKeepalive(backendHealthy))
			_, err = srv.ServerTLSHandshake(stls)
			require.NoError(t, err)
			require.NoError(t, srv.SetKeepalive(backendUnhealthy))
			require.NoError(t, srv.WritePacket([]byte{0, 1, 2}, true))
			time.Sleep(3*time.Second + 100*time.Millisecond)
			_, err := srv.ReadPacket()
			require.NoError(t, err)
		},
		1,
	)
}

func TestPredefinedPacket(t *testing.T) {
	testTCPConn(t,
		func(t *testing.T, cli *PacketIO) {
			data, err := cli.ReadPacket()
			require.NoError(t, err)
			merr := ParseErrorPacket(data)
			require.Equal(t, uint16(mysql.ER_UNKNOWN_ERROR), merr.Code)
			require.Equal(t, "Unknown error", merr.Message)

			data, err = cli.ReadPacket()
			require.NoError(t, err)
			merr = ParseErrorPacket(data)
			require.Equal(t, uint16(mysql.ER_UNKNOWN_ERROR), merr.Code)
			require.Equal(t, "test error", merr.Message)

			data, err = cli.ReadPacket()
			require.NoError(t, err)
			status := ParseOKPacket(data)
			require.Equal(t, uint16(100), status)
		},
		func(t *testing.T, srv *PacketIO) {
			require.NoError(t, srv.WriteErrPacket(mysql.NewDefaultError(mysql.ER_UNKNOWN_ERROR)))
			require.NoError(t, srv.WriteErrPacket(mysql.NewError(mysql.ER_UNKNOWN_ERROR, "test error")))
			require.NoError(t, srv.WriteOKPacket(100, OKHeader))
		},
		1,
	)
}

// Test the combination of proxy, tls and compress.
func TestProxyTLSCompress(t *testing.T) {
	stls, ctls, err := security.CreateTLSConfigForTest()
	require.NoError(t, err)
	addr, p := mockProxy(t)
	ch := make(chan []byte)
	write := func(p *PacketIO, data []byte) {
		outBytes := p.OutBytes()
		require.NoError(t, p.WritePacket(data, true))
		ch <- data
		require.Greater(t, p.OutBytes(), outBytes)
		require.True(t, p.IsPeerActive())
		require.NotEmpty(t, p.RemoteAddr().String())
	}
	read := func(p *PacketIO) {
		inBytes := p.InBytes()
		data := <-ch
		pkt, err := p.ReadPacket()
		require.NoError(t, err)
		require.Equal(t, data, pkt)
		require.Greater(t, p.InBytes(), inBytes)
		require.True(t, p.IsPeerActive())
		require.NotEmpty(t, p.RemoteAddr().String())
	}
	for _, enableCompress := range []bool{true, false} {
		for _, enableTLS := range []bool{true, false} {
			for _, enableProxy := range []bool{true, false} {
				testTCPConn(t, func(t *testing.T, cli *PacketIO) {
					if enableProxy {
						cli.EnableProxyClient(p)
					}
					write(cli, []byte("test1"))
					if enableTLS {
						require.NoError(t, cli.ClientTLSHandshake(ctls))
						require.True(t, cli.TLSConnectionState().HandshakeComplete)
					}
					read(cli)
					if enableCompress {
						cli.ResetSequence()
						require.NoError(t, cli.SetCompressionAlgorithm(CompressionZlib, 0))
					}
					write(cli, []byte("test3"))
					read(cli)
					// make sure the peer won't quit in advance
					ch <- nil
				}, func(t *testing.T, srv *PacketIO) {
					if enableProxy {
						srv.EnableProxyServer()
					}
					read(srv)
					if enableProxy {
						require.Equal(t, addr.String(), srv.RemoteAddr().String())
						require.Equal(t, addr.String(), srv.Proxy().SrcAddress.String())
					}
					if enableTLS {
						state, err := srv.ServerTLSHandshake(stls)
						require.NoError(t, err)
						require.True(t, state.HandshakeComplete)
						require.True(t, srv.TLSConnectionState().HandshakeComplete)
					}
					write(srv, []byte("test2"))
					if enableCompress {
						srv.ResetSequence()
						require.NoError(t, srv.SetCompressionAlgorithm(CompressionZlib, 0))
					}
					read(srv)
					write(srv, []byte("test4"))
					// make sure the peer won't quit in advance
					<-ch
				}, 1)
			}
		}
	}
}

// Test the sequence is correct with the compression protocol.
func TestPacketSequence(t *testing.T) {
	write := func(p *PacketIO, flush bool) {
		require.NoError(t, p.WritePacket([]byte{0}, flush))
	}
	read := func(p *PacketIO) {
		_, err := p.ReadPacket()
		require.NoError(t, err)
	}
	loops := 1024
	testTCPConn(t,
		func(t *testing.T, cli *PacketIO) {
			require.NoError(t, cli.SetCompressionAlgorithm(CompressionZlib, 0))
			read(cli)
			// uncompressed sequence = compressed sequence
			write(cli, false)
			write(cli, true)
			// uncompressed sequence wraps around (1000 writes + 1 flush)
			for i := 0; i < loops; i++ {
				write(cli, false)
			}
			require.NoError(t, cli.Flush())
			// compressed sequence wraps around (1000 writes + 1000 flushes)
			for i := 0; i < loops; i++ {
				write(cli, true)
			}
			// reset sequence
			cli.ResetSequence()
			write(cli, true)
		},
		func(t *testing.T, srv *PacketIO) {
			require.NoError(t, srv.SetCompressionAlgorithm(CompressionZlib, 0))
			write(srv, true)
			// uncompressed sequence = compressed sequence
			read(srv)
			read(srv)
			// uncompressed sequence wraps around
			for i := 0; i < loops; i++ {
				read(srv)
			}
			// compressed sequence wraps around
			for i := 0; i < loops; i++ {
				read(srv)
			}
			// reset sequence
			srv.ResetSequence()
			read(srv)
		},
		1,
	)
}

func TestForwardUntil(t *testing.T) {
	stls, ctls, err := security.CreateTLSConfigForTest()
	require.NoError(t, err)
	_, p := mockProxy(t)
	srvCh := make(chan *PacketIO)
	exitCh := make(chan struct{})
	prepareClient := func(enableProxy, enableTLS, enableCompress bool, cli *PacketIO) {
		if enableProxy {
			cli.EnableProxyClient(p)
		}
		if enableTLS {
			require.NoError(t, cli.ClientTLSHandshake(ctls))
			require.True(t, cli.TLSConnectionState().HandshakeComplete)
		}
		if enableCompress {
			cli.ResetSequence()
			require.NoError(t, cli.SetCompressionAlgorithm(CompressionZlib, 0))
		}
	}
	prepareServer := func(enableProxy, enableTLS, enableCompress bool, srv *PacketIO) {
		if enableProxy {
			srv.EnableProxyServer()
		}
		if enableTLS {
			state, err := srv.ServerTLSHandshake(stls)
			require.NoError(t, err)
			require.True(t, state.HandshakeComplete)
			require.True(t, srv.TLSConnectionState().HandshakeComplete)
		}
		if enableCompress {
			srv.ResetSequence()
			require.NoError(t, srv.SetCompressionAlgorithm(CompressionZlib, 0))
		}
	}
	for _, enableCompress := range []bool{true, false} {
		for _, enableTLS := range []bool{true, false} {
			for _, enableProxy := range []bool{true, false} {
				var wg waitgroup.WaitGroup
				loops := 100
				// client1 writes to server1
				// server1 forwards to server2
				// server2 writes to client2
				wg.Run(func() {
					testTCPConn(t,
						func(t *testing.T, cli *PacketIO) {
							prepareClient(enableProxy, enableTLS, enableCompress, cli)
							for i := 0; i <= loops; i++ {
								require.NoError(t, cli.WritePacket([]byte{byte(i)}, true))
							}
						},
						func(t *testing.T, srv1 *PacketIO) {
							prepareServer(enableProxy, enableTLS, enableCompress, srv1)
							srv2 := <-srvCh
							err := srv1.ForwardUntil(srv2, func(firstByte byte, firstPktLen int) (bool, bool) {
								return firstByte == byte(loops) && firstPktLen == 1, true
							}, func(response []byte) error {
								require.Equal(t, []byte{byte(loops)}, response)
								return srv2.Flush()
							})
							require.NoError(t, err)
							exitCh <- struct{}{}
						},
						1,
					)
				})
				wg.Run(func() {
					testTCPConn(t,
						func(t *testing.T, cli *PacketIO) {
							prepareClient(enableProxy, enableTLS, enableCompress, cli)
							for i := 0; i < loops; i++ {
								data, err := cli.ReadPacket()
								require.NoError(t, err)
								require.Equal(t, []byte{byte(i)}, data)
							}
						},
						func(t *testing.T, srv2 *PacketIO) {
							prepareServer(enableProxy, enableTLS, enableCompress, srv2)
							srvCh <- srv2
							<-exitCh
						},
						1,
					)
				})
				wg.Wait()
			}
		}
	}
}

func TestForwardUntilLongData(t *testing.T) {
	srvCh := make(chan *PacketIO)
	exitCh := make(chan struct{})
	var wg waitgroup.WaitGroup
	sizes := []int{DefaultConnBufferSize, DefaultConnBufferSize * 2, MaxPayloadLen - 1, MaxPayloadLen, MaxPayloadLen + 1, MaxPayloadLen*2 - 1, MaxPayloadLen * 2}
	wg.Run(func() {
		testTCPConn(t,
			func(t *testing.T, cli *PacketIO) {
				for i, size := range sizes {
					data := make([]byte, size)
					data[0] = byte(i)
					require.NoError(t, cli.WritePacket(data, true))
				}
			},
			func(t *testing.T, srv1 *PacketIO) {
				srv2 := <-srvCh
				err := srv1.ForwardUntil(srv2, func(firstByte byte, firstPktLen int) (bool, bool) {
					return firstByte >= byte(len(sizes)-1), true
				}, func(response []byte) error {
					require.Len(t, response, sizes[len(sizes)-1])
					return srv2.Flush()
				})
				require.NoError(t, err)
				sum := 0
				for _, size := range sizes {
					sum += size + 4
				}
				require.GreaterOrEqual(t, srv1.InBytes(), uint64(sum))
				require.Equal(t, srv1.InBytes(), srv2.OutBytes())
				exitCh <- struct{}{}
			},
			1,
		)
	})
	wg.Run(func() {
		testTCPConn(t,
			func(t *testing.T, cli *PacketIO) {
				for i, size := range sizes {
					data, err := cli.ReadPacket()
					require.NoError(t, err)
					require.Equal(t, byte(i), data[0])
					require.Len(t, data, size)
				}
			},
			func(t *testing.T, srv2 *PacketIO) {
				srvCh <- srv2
				<-exitCh
			},
			1,
		)
	})
	wg.Wait()
}

func BenchmarkWritePacket(b *testing.B) {
	b.ReportAllocs()
	cli, srv := net.Pipe()
	var wg waitgroup.WaitGroup
	wg.Run(func() {
		packetIO := NewPacketIO(srv, nil, DefaultConnBufferSize)
		for {
			if _, err := packetIO.ReadPacket(); err != nil {
				break
			}
		}
		_ = srv.Close()
	})
	packetIO := NewPacketIO(cli, nil, DefaultConnBufferSize)
	data := make([]byte, 100)
	for i := 0; i < b.N; i++ {
		if err := packetIO.WritePacket(data, false); err != nil {
			b.Fatal(err)
		}
	}
	_ = packetIO.Close()
	wg.Wait()
}

func BenchmarkReadPacket(b *testing.B) {
	b.ReportAllocs()
	cli, srv := net.Pipe()
	var wg waitgroup.WaitGroup
	wg.Run(func() {
		b := make([]byte, 1024*1024)
		packetIO := NewPacketIO(srv, nil, 1024*1024)
		for {
			if err := packetIO.WritePacket(b, true); err != nil {
				break
			}
		}
		_ = srv.Close()
	})
	packetIO := NewPacketIO(cli, nil, DefaultConnBufferSize)
	for i := 0; i < b.N; i++ {
		if _, err := packetIO.ReadPacket(); err != nil {
			b.Fatal(err)
		}
	}
	_ = packetIO.Close()
	wg.Wait()
}

func BenchmarkForwardWithReadWrite(b *testing.B) {
	loops := 100
	runForwardBenchmark(b, func(packetIO1, packetIO2 *PacketIO) {
		for j := 0; j < loops; j++ {
			data, err := packetIO1.ReadPacket()
			if err != nil {
				b.Fatal(err)
			}
			if err = packetIO2.WritePacket(data, j == loops-1); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkForwardUntil(b *testing.B) {
	loops := 100
	runForwardBenchmark(b, func(packetIO1, packetIO2 *PacketIO) {
		j := 0
		err := packetIO1.ForwardUntil(packetIO2, func(firstByte byte, firstPktLen int) (bool, bool) {
			j++
			if j == loops {
				if err := packetIO2.Flush(); err != nil {
					b.Fatal(err)
				}
				return true, false
			}
			return false, false
		}, nil)
		if err != nil {
			b.Fatal(err)
		}
	})
}

func runForwardBenchmark(b *testing.B, f func(packetIO1, packetIO2 *PacketIO)) {
	b.ReportAllocs()
	cli1, srv1 := net.Pipe()
	cli2, srv2 := net.Pipe()
	var wg waitgroup.WaitGroup
	// cli1: writer
	wg.Run(func() {
		b := make([]byte, 128)
		packetIO := NewPacketIO(cli1, nil, DefaultConnBufferSize)
		for i := 0; ; i++ {
			if err := packetIO.WritePacket(b, i%10 == 0); err != nil {
				break
			}
		}
		_ = cli1.Close()
	})
	// cli2: reader
	wg.Run(func() {
		packetIO := NewPacketIO(cli2, nil, DefaultConnBufferSize)
		for {
			if _, err := packetIO.ReadPacket(); err != nil {
				break
			}
		}
		_ = cli2.Close()
	})
	packetIO1 := NewPacketIO(srv1, nil, DefaultConnBufferSize)
	packetIO2 := NewPacketIO(srv2, nil, DefaultConnBufferSize)
	for i := 0; i < b.N; i++ {
		f(packetIO1, packetIO2)
	}
	_ = packetIO1.Close()
	_ = packetIO2.Close()
	wg.Wait()
}
