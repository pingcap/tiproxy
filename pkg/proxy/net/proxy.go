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
	"bytes"
	"io"
	"net"

	"github.com/pingcap/TiProxy/lib/util/errors"
)

type ProxyVersion int

const (
	ProxyVersion2 ProxyVersion = iota + 2
)

type ProxyCommand int

const (
	ProxyCommandLocal ProxyCommand = iota
	ProxyCommandProxy
)

type ProxyAddressFamily int

const (
	ProxyAFUnspec ProxyAddressFamily = iota
	ProxyAFINet
	ProxyAFINet6
	ProxyAFUnix
)

type ProxyNetwork int

const (
	ProxyNetworkUnspec ProxyNetwork = iota
	ProxyNetworkStream
	ProxyNetworkDgram
)

type ProxyTlvType int

const (
	ProxyTlvALPN ProxyTlvType = iota + 0x01
	ProxyTlvAuthority
	ProxyTlvCRC32C
	ProxyTlvNoop
	ProxyTlvUniqueID
	ProxyTlvSSL ProxyTlvType = iota + 0x20
	ProxyTlvSSLCN
	ProxyTlvSSLCipher
	ProxyTlvSSLSignALG
	ProxyTlvSSLKeyALG
	ProxyTlvNetns ProxyTlvType = iota + 0x30
)

type ProxyTlv struct {
	content []byte
	typ     ProxyTlvType
}

type Proxy struct {
	SrcAddress net.Addr
	DstAddress net.Addr
	TLV        []ProxyTlv
	Version    ProxyVersion
	Command    ProxyCommand
}

func (p *Proxy) ToBytes() []byte {
	buf := make([]byte, len(proxyV2Magic)+4)
	ptr := copy(buf, proxyV2Magic)

	buf[ptr] = byte(p.Version<<4) | byte(p.Command&0xF)
	ptr++

	var length int
	addressFamily := ProxyAFUnspec
	network := ProxyNetworkUnspec
	switch sadd := p.SrcAddress.(type) {
	case *net.TCPAddr:
		length = len(sadd.IP)*2 + 4
		addressFamily = ProxyAFINet
		if length == 36 {
			addressFamily = ProxyAFINet6
		}
		network = ProxyNetworkStream
		dadd := p.DstAddress.(*net.TCPAddr)
		buf = append(buf, sadd.IP...)
		buf = append(buf, dadd.IP...)
		buf = append(buf, byte(sadd.Port>>8), byte(sadd.Port))
		buf = append(buf, byte(dadd.Port>>8), byte(dadd.Port))
	case *net.UDPAddr:
		length = len(sadd.IP)*2 + 4
		addressFamily = ProxyAFINet
		if length == 36 {
			addressFamily = ProxyAFINet6
		}
		network = ProxyNetworkDgram
		dadd := p.DstAddress.(*net.UDPAddr)
		buf = append(buf, sadd.IP...)
		buf = append(buf, dadd.IP...)
		buf = append(buf, byte(sadd.Port>>8), byte(sadd.Port))
		buf = append(buf, byte(dadd.Port>>8), byte(dadd.Port))
	case *net.UnixAddr:
		length = 216
		addressFamily = ProxyAFUnix
		switch sadd.Net {
		case "unix":
			network = ProxyNetworkStream
		case "unixdgram":
			network = ProxyNetworkDgram
		}
		dadd := p.DstAddress.(*net.UnixAddr)
		buf = append(buf, []byte(sadd.Name)...)
		buf = append(buf, []byte(dadd.Name)...)
	}
	buf[ptr] = byte(addressFamily<<4) | byte(network&0xF)
	ptr++

	for _, tlv := range p.TLV {
		buf = append(buf, byte(tlv.typ))
		tlen := len(tlv.content)
		buf = append(buf, byte(tlen>>8), byte(tlen))
		buf = append(buf, tlv.content...)
		length += 3 + tlen
	}

	buf[ptr] = byte(length >> 8)
	buf[ptr+1] = byte(length)

	return buf
}

func (p *PacketIO) parseProxyV2() (*Proxy, error) {
	rem, err := p.buf.Peek(8)
	if err != nil {
		return nil, errors.WithStack(errors.Wrap(ErrReadConn, err))
	}
	if !bytes.Equal(rem, proxyV2Magic[4:]) {
		return nil, nil
	}

	// yes, it is proxyV2
	_, err = p.buf.Discard(8)
	if err != nil {
		return nil, errors.WithStack(errors.Wrap(ErrReadConn, err))
	}

	var hdr [4]byte

	if _, err := io.ReadFull(p.buf, hdr[:]); err != nil {
		return nil, errors.WithStack(err)
	}

	m := &Proxy{}
	m.Version = ProxyVersion(hdr[0] >> 4)
	m.Command = ProxyCommand(hdr[0] & 0xF)

	buf := make([]byte, int(hdr[2])<<8|int(hdr[3]))
	if _, err := io.ReadFull(p.buf, buf); err != nil {
		return nil, errors.WithStack(err)
	}

	addressFamily := ProxyAddressFamily(hdr[1] >> 4)
	network := ProxyNetwork(hdr[1] & 0xF)
	switch addressFamily {
	case ProxyAFINet:
		fallthrough
	case ProxyAFINet6:
		length := 4
		if addressFamily == ProxyAFINet6 {
			length = 16
		}
		if len(buf) < length*2+4 {
			// TODO: logging
			break
		}
		saddr := net.IP(buf[:length])
		daddr := net.IP(buf[length : length*2])
		sport := int(buf[2*length])<<8 | int(buf[2*length+1])
		dport := int(buf[2*length+2])<<8 | int(buf[2*length+3])
		switch network {
		case ProxyNetworkStream:
			m.SrcAddress = &net.TCPAddr{
				IP:   saddr,
				Port: sport,
			}
			m.DstAddress = &net.TCPAddr{
				IP:   daddr,
				Port: dport,
			}
		case ProxyNetworkDgram:
			m.SrcAddress = &net.UDPAddr{
				IP:   saddr,
				Port: sport,
			}
			m.DstAddress = &net.UDPAddr{
				IP:   daddr,
				Port: dport,
			}
		default:
			// TODO: logging
		}
		buf = buf[length*2+4:]
	case ProxyAFUnix:
		if len(buf) < 216 {
			// TODO: logging
			break
		}
		saddr := string(buf[:108])
		daddr := string(buf[108:216])
		switch network {
		case ProxyNetworkStream:
			m.SrcAddress = &net.UnixAddr{
				Name: saddr,
				Net:  "unix",
			}
			m.DstAddress = &net.UnixAddr{
				Name: daddr,
				Net:  "unix",
			}
		case ProxyNetworkDgram:
			m.SrcAddress = &net.UnixAddr{
				Name: saddr,
				Net:  "unixdgram",
			}
			m.DstAddress = &net.UnixAddr{
				Name: daddr,
				Net:  "unixdgram",
			}
		default:
			// TODO: logging
		}
		buf = buf[216:]
	default:
		buf = buf[len(buf):]
	}

	for len(buf) >= 3 {
		typ := ProxyTlvType(buf[0])
		length := int(buf[1])<<8 | int(buf[2])
		if len(buf) < length+3 {
			length = len(buf) - 3
		}
		m.TLV = append(m.TLV, ProxyTlv{
			typ:     typ,
			content: buf[3 : 3+length],
		})
		buf = buf[3+length:]
	}

	return m, nil
}

// WriteProxyV2 should only be called at the beginning of connection, before any write operations.
func (p *PacketIO) WriteProxyV2(m *Proxy) error {
	if _, err := io.Copy(p.buf, bytes.NewReader(m.ToBytes())); err != nil {
		return errors.WithStack(errors.Wrap(ErrWriteConn, err))
	}
	// according to the spec, we better flush to avoid server hanging
	return p.Flush()
}
