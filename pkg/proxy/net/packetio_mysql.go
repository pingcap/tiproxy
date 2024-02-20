// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"encoding/binary"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"go.uber.org/zap"
)

var (
	ErrSaltNotLongEnough = errors.New("salt is not long enough")
)

// WriteInitialHandshake writes an initial handshake as a server.
// It's used for tenant-aware routing and testing.
func (p *PacketIO) WriteInitialHandshake(capability Capability, salt [20]byte, authPlugin string, serverVersion string, connID uint64) error {
	saltLen := len(salt)
	data := make([]byte, 0, 128)

	// min version 10
	data = append(data, 10)
	// server version[NUL]
	data = append(data, serverVersion...)
	data = append(data, 0)
	// connection id
	data = append(data, byte(connID), byte(connID>>8), byte(connID>>16), byte(connID>>24))
	// auth-plugin-data-part-1
	data = append(data, salt[0:8]...)
	// filler [00]
	data = append(data, 0)
	// capability flag lower 2 bytes, using default capability here
	data = append(data, byte(capability), byte(capability>>8))
	// charset
	data = append(data, Collation)
	// status
	data = DumpUint16(data, Status)
	// capability flag upper 2 bytes, using default capability here
	data = append(data, byte(capability>>16), byte(capability>>24))
	// length of auth-plugin-data
	data = append(data, byte(saltLen+1))
	// reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	// auth-plugin-data-part-2
	data = append(data, salt[8:saltLen]...)
	data = append(data, 0)
	// auth-plugin name
	data = append(data, []byte(authPlugin)...)
	data = append(data, 0)

	return p.WritePacket(data, true)
}

// WriteSwitchRequest writes a switch request to the client. It's only for testing.
func (p *PacketIO) WriteSwitchRequest(authPlugin string, salt [20]byte) error {
	length := 1 + len(authPlugin) + 1 + len(salt) + 1
	data := make([]byte, 0, length)
	// check https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_request.html
	data = append(data, byte(AuthSwitchHeader))
	data = append(data, authPlugin...)
	data = append(data, 0x00)
	data = append(data, salt[:]...)
	data = append(data, 0x00)
	return p.WritePacket(data, true)
}

func (p *PacketIO) WriteShaCommand() error {
	return p.WritePacket([]byte{ShaCommand, FastAuthFail}, true)
}

func (p *PacketIO) ReadSSLRequestOrHandshakeResp() (pkt []byte, isSSL bool, err error) {
	pkt, err = p.ReadPacket()
	if err != nil {
		return
	}

	if len(pkt) < 32 {
		p.logger.Error("got malformed handshake response", zap.ByteString("packetData", pkt))
		err = mysql.ErrMalformPacket
		return
	}

	capability := Capability(binary.LittleEndian.Uint32(pkt[:4]))
	isSSL = capability&ClientSSL != 0
	return
}

// WriteErrPacket writes an Error packet.
func (p *PacketIO) WriteErrPacket(merr *mysql.MyError) error {
	data := make([]byte, 0, 9+len(merr.Message))
	data = append(data, ErrHeader.Byte())
	data = append(data, byte(merr.Code), byte(merr.Code>>8))
	// ClientProtocol41 is always enabled.
	data = append(data, '#')
	data = append(data, merr.State...)
	data = append(data, merr.Message...)
	return p.WritePacket(data, true)
}

// WriteOKPacket writes an OK packet. It's only for testing.
func (p *PacketIO) WriteOKPacket(status uint16, header Header) error {
	data := make([]byte, 0, 7)
	data = append(data, header.Byte())
	data = append(data, 0, 0)
	// ClientProtocol41 is always enabled.
	data = DumpUint16(data, status)
	data = append(data, 0, 0)
	return p.WritePacket(data, true)
}

// WriteEOFPacket writes an EOF packet. It's only for testing.
func (p *PacketIO) WriteEOFPacket(status uint16) error {
	data := make([]byte, 0, 5)
	data = append(data, EOFHeader.Byte())
	data = append(data, 0, 0)
	// ClientProtocol41 is always enabled.
	data = DumpUint16(data, status)
	return p.WritePacket(data, true)
}

// WriteUserError writes an unknown error to the client.
func (p *PacketIO) WriteUserError(err error) {
	if err == nil {
		return
	}
	myErr := mysql.NewError(mysql.ER_UNKNOWN_ERROR, err.Error())
	if writeErr := p.WriteErrPacket(myErr); writeErr != nil {
		p.logger.Error("writing error to client failed", zap.NamedError("mysql_err", err), zap.NamedError("write_err", writeErr))
	}
}
