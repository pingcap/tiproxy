// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"bytes"
	"crypto/tls"
	"net"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/proxy/proxyprotocol"
)

var _ pnet.PacketIO = (*packetIO)(nil)

type packetIO struct {
	request  []byte
	resp     bytes.Buffer
	saveResp bool // Discard responses by default because the result may be huge and causes OOM.
}

func newPacketIO() *packetIO {
	return &packetIO{}
}

// ReadPacket implements net.PacketIO.
func (p *packetIO) ReadPacket() (data []byte, err error) {
	if p.request == nil {
		return nil, errors.New("no request")
	}
	return p.request, nil
}

// WritePacket implements net.PacketIO.
func (p *packetIO) WritePacket(data []byte, flush bool) error {
	if !p.saveResp {
		return nil
	}
	if _, err := p.resp.Write(data); err != nil {
		return err
	}
	return nil
}

// Flush implements net.PacketIO.
func (p *packetIO) Flush() error {
	return nil
}

func (p *packetIO) Reset() {
	p.resp.Reset()
	p.saveResp = false
	p.request = nil
}

func (p *packetIO) GetResp() []byte {
	return p.resp.Bytes()
}

// ForwardUntil implements net.PacketIO.
// ForwardUntil won't be called on the client side, so no need to implement it.
func (p *packetIO) ForwardUntil(dest pnet.PacketIO, isEnd func(firstByte byte, firstPktLen int) (end bool, needData bool), process func(response []byte) error) error {
	return errors.New("command not supported")
}

// ForwardPacketTo implements [net.PacketIO].
func (p *packetIO) ForwardPacketTo(destIO pnet.PacketIO, captureLimit int) (data []byte, err error) {
	if p.request == nil {
		return nil, errors.New("no request")
	}
	if err := destIO.WritePacket(p.request, true); err != nil {
		return nil, err
	}
	if captureLimit > 0 {
		n := min(captureLimit, len(p.request))
		return p.request[:n], nil
	}
	return nil, nil
}

// PeekPacketFirstByte implements [net.PacketIO].
func (p *packetIO) PeekPacketFirstByte() (firstByte byte, firstPktLen int, err error) {
	if p.request == nil {
		return 0, 0, errors.New("no request")
	}
	return p.request[0], len(p.request), nil
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

// ProxyAddr implements net.PacketIO.
func (p *packetIO) ProxyAddr() net.Addr {
	return &net.TCPAddr{}
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
