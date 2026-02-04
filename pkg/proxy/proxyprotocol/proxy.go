// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package proxyprotocol

import (
	"io"
	"net"
)

var (
	MagicV2 = []byte{0xD, 0xA, 0xD, 0xA, 0x0, 0xD, 0xA, 0x51, 0x55, 0x49, 0x54, 0xA}
)

func unwrapOriginAddr(addr net.Addr) net.Addr {
	for {
		v, ok := addr.(AddressWrapper)
		if !ok {
			return addr
		}
		addr = v.Unwrap()
	}
}

func (p *Proxy) ToBytes() ([]byte, error) {
	magicLen := len(MagicV2)
	buf := make([]byte, magicLen+4)
	_ = copy(buf, MagicV2)
	buf[magicLen] = byte(p.Version<<4) | byte(p.Command&0xF)

	addressFamily := ProxyAFUnspec
	network := ProxyNetworkUnspec

	srcAddr := unwrapOriginAddr(p.SrcAddress)
	dstAddr := unwrapOriginAddr(p.DstAddress)

	switch sadd := srcAddr.(type) {
	case *net.TCPAddr:
		dadd, ok := dstAddr.(*net.TCPAddr)
		if !ok {
			return nil, ErrAddressFamilyMismatch
		}
		saddUnifiedIP, daddUnifiedIP := unifyIPFamily(sadd.IP, dadd.IP)

		addressFamily = ProxyAFINet
		if len(saddUnifiedIP) == net.IPv6len {
			addressFamily = ProxyAFINet6
		}
		network = ProxyNetworkStream
		buf = append(buf, saddUnifiedIP...)
		buf = append(buf, daddUnifiedIP...)
		buf = append(buf, byte(sadd.Port>>8), byte(sadd.Port))
		buf = append(buf, byte(dadd.Port>>8), byte(dadd.Port))
	case *net.UDPAddr:
		dadd, ok := dstAddr.(*net.UDPAddr)
		if !ok {
			return nil, ErrAddressFamilyMismatch
		}
		saddUnifiedIP, daddUnifiedIP := unifyIPFamily(sadd.IP, dadd.IP)

		addressFamily = ProxyAFINet
		if len(saddUnifiedIP) == net.IPv6len {
			addressFamily = ProxyAFINet6
		}
		network = ProxyNetworkDgram
		buf = append(buf, saddUnifiedIP...)
		buf = append(buf, daddUnifiedIP...)
		buf = append(buf, byte(sadd.Port>>8), byte(sadd.Port))
		buf = append(buf, byte(dadd.Port>>8), byte(dadd.Port))
	case *net.UnixAddr:
		addressFamily = ProxyAFUnix
		switch sadd.Net {
		case "unix":
			network = ProxyNetworkStream
		case "unixdgram":
			network = ProxyNetworkDgram
		}
		dadd, ok := dstAddr.(*net.UnixAddr)
		if !ok {
			return nil, ErrAddressFamilyMismatch
		}
		buf = append(buf, []byte(sadd.Name)...)
		buf = append(buf, []byte(dadd.Name)...)
	}
	buf[magicLen+1] = byte(addressFamily<<4) | byte(network&0xF)

	for _, tlv := range p.TLV {
		buf = append(buf, byte(tlv.Typ))
		tlen := len(tlv.Content)
		buf = append(buf, byte(tlen>>8), byte(tlen))
		buf = append(buf, tlv.Content...)
	}

	length := len(buf) - 4 - magicLen
	buf[magicLen+2] = byte(length >> 8)
	buf[magicLen+3] = byte(length)

	return buf, nil
}

// unifyIPFamily unifies the IP family of ip1 and ip2.
// If both of them are IPv4 (or IPv4 mapped IPv6), return the IPv4 addresses.
// Else, convert both of them to IPv6 and return.
func unifyIPFamily(ip1 net.IP, ip2 net.IP) (net.IP, net.IP) {
	ip1To4 := ip1.To4()
	ip2To4 := ip2.To4()
	if ip1To4 != nil && ip2To4 != nil {
		return ip1To4, ip2To4
	}

	return ip1.To16(), ip2.To16()
}

func ParseProxyV2(rd io.Reader) (m *Proxy, n int, err error) {
	var hdr [4]byte

	if _, err = io.ReadFull(rd, hdr[:]); err != nil {
		return
	}
	n += 4

	m = &Proxy{}
	m.Version = ProxyVersion(hdr[0] >> 4)
	m.Command = ProxyCommand(hdr[0] & 0xF)

	buf := make([]byte, int(hdr[2])<<8|int(hdr[3]))
	if _, err = io.ReadFull(rd, buf); err != nil {
		return
	}
	n += len(buf)

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
			Typ:     typ,
			Content: buf[3 : 3+length],
		})
		buf = buf[3+length:]
	}

	return
}
