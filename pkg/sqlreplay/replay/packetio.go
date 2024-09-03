// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"bytes"
	"crypto/tls"
	"net"

	"github.com/pingcap/tiproxy/lib/config"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/proxy/proxyprotocol"
)

var _ pnet.PacketIO = (*packetIO)(nil)

type packetIO struct {
	cli2SrvCh  <-chan []byte
	srv2CliCh  chan<- []byte
	srv2CliBuf bytes.Buffer
}

func newPacketIO(cli2SrvCh <-chan []byte, srv2CliCh chan<- []byte) *packetIO {
	return &packetIO{
		cli2SrvCh: cli2SrvCh,
		srv2CliCh: srv2CliCh,
	}
}

// ReadPacket implements net.PacketIO.
func (p *packetIO) ReadPacket() (data []byte, err error) {
	data = <-p.cli2SrvCh
	return data, nil
}

// WritePacket implements net.PacketIO.
func (p *packetIO) WritePacket(data []byte, flush bool) (err error) {
	if _, err := p.srv2CliBuf.Write(data); err != nil {
		return err
	}
	if flush {
		return p.Flush()
	}
	return nil
}

// Flush implements net.PacketIO.
func (p *packetIO) Flush() error {
	p.srv2CliCh <- p.srv2CliBuf.Bytes()
	p.srv2CliBuf.Reset()
	return nil
}

// ForwardUntil implements net.PacketIO.
// ForwardUntil won't be called on the client side, so no need to implement it.
func (p *packetIO) ForwardUntil(dest pnet.PacketIO, isEnd func(firstByte byte, firstPktLen int) (end bool, needData bool), process func(response []byte) error) error {
	panic("unimplemented")
}

// ClientTLSHandshake implements net.PacketIO.
func (p *packetIO) ClientTLSHandshake(tlsConfig *tls.Config) error {
	return nil
}

// Close implements net.PacketIO.
func (p *packetIO) Close() error {
	return nil
}

// EnableProxyClient implements net.PacketIO.
func (p *packetIO) EnableProxyClient(proxy *proxyprotocol.Proxy) {
}

// EnableProxyServer implements net.PacketIO.
func (p *packetIO) EnableProxyServer() {
}

// GetSequence implements net.PacketIO.
func (p *packetIO) GetSequence() uint8 {
	return 0
}

// GracefulClose implements net.PacketIO.
func (p *packetIO) GracefulClose() error {
	return nil
}

// InBytes implements net.PacketIO.
func (p *packetIO) InBytes() uint64 {
	return 0
}

// InPackets implements net.PacketIO.
func (p *packetIO) InPackets() uint64 {
	return 0
}

// IsPeerActive implements net.PacketIO.
func (p *packetIO) IsPeerActive() bool {
	return true
}

// LastKeepAlive implements net.PacketIO.
func (p *packetIO) LastKeepAlive() config.KeepAlive {
	return config.KeepAlive{}
}

// LocalAddr implements net.PacketIO.
func (p *packetIO) LocalAddr() net.Addr {
	return &net.TCPAddr{}
}

// OutBytes implements net.PacketIO.
func (p *packetIO) OutBytes() uint64 {
	return 0
}

// OutPackets implements net.PacketIO.
func (p *packetIO) OutPackets() uint64 {
	return 0
}

// Proxy implements net.PacketIO.
func (p *packetIO) Proxy() *proxyprotocol.Proxy {
	return nil
}

// RemoteAddr implements net.PacketIO.
func (p *packetIO) RemoteAddr() net.Addr {
	return &net.TCPAddr{}
}

// ResetSequence implements net.PacketIO.
func (p *packetIO) ResetSequence() {
}

// ServerTLSHandshake implements net.PacketIO.
func (p *packetIO) ServerTLSHandshake(tlsConfig *tls.Config) (tls.ConnectionState, error) {
	return tls.ConnectionState{}, nil
}

// SetCompressionAlgorithm implements net.PacketIO.
func (p *packetIO) SetCompressionAlgorithm(algorithm pnet.CompressAlgorithm, zstdLevel int) error {
	return nil
}

// SetKeepalive implements net.PacketIO.
func (p *packetIO) SetKeepalive(cfg config.KeepAlive) error {
	return nil
}

// TLSConnectionState implements net.PacketIO.
func (p *packetIO) TLSConnectionState() tls.ConnectionState {
	return tls.ConnectionState{}
}

func (p *packetIO) ApplyOpts(opts ...pnet.PacketIOption) {
}
