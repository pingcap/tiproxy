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

	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/tidb/parser/mysql"
)

var (
	ErrSaltNotLongEnough = errors.New("salt is not long enough")
)

// WriteInitialHandshake writes an initial handshake as a server.
// It's used for tenant-aware routing and testing.
func (p *PacketIO) WriteInitialHandshake(capability Capability, salt []byte, authPlugin string) error {
	saltLen := len(salt)
	if saltLen < 8 {
		return ErrSaltNotLongEnough
	} else if saltLen > 20 {
		saltLen = 20
	}

	data := make([]byte, 0, 128)

	// min version 10
	data = append(data, 10)
	// server version[NUL]
	data = append(data, testServerVersion...)
	data = append(data, 0)
	// connection id
	data = append(data, byte(testConnID), byte(testConnID>>8), byte(testConnID>>16), byte(testConnID>>24))
	// auth-plugin-data-part-1
	data = append(data, salt[0:8]...)
	// filler [00]
	data = append(data, 0)
	// capability flag lower 2 bytes, using default capability here
	data = append(data, byte(capability), byte(capability>>8))
	// charset
	data = append(data, testCollation)
	// status
	data = DumpUint16(data, testStatus)
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
func (p *PacketIO) WriteSwitchRequest(authPlugin string, salt []byte) error {
	length := 1 + len(authPlugin) + 1 + len(salt) + 1
	data := make([]byte, 0, length)
	data = append(data, mysql.AuthSwitchRequest)
	data = append(data, []byte(authPlugin)...)
	data = append(data, 0x00)
	data = append(data, salt...)
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
		err = errors.WithStack(errors.Errorf("%w: but got less than 32 bytes", ErrExpectSSLRequest))
		return
	}

	capability := Capability(binary.LittleEndian.Uint32(pkt[:4]))
	isSSL = capability&ClientSSL != 0
	return
}

// WriteErrPacket writes an Error packet.
func (p *PacketIO) WriteErrPacket(merr *mysql.SQLError) error {
	data := make([]byte, 0, 4+len(merr.Message)+len(merr.State))
	data = append(data, mysql.ErrHeader)
	data = append(data, byte(merr.Code), byte(merr.Code>>8))
	// ClientProtocol41 must be enabled.
	data = append(data, '#')
	data = append(data, merr.State...)
	data = append(data, merr.Message...)
	return p.WritePacket(data, true)
}

// WriteOKPacket writes an OK packet. It's only for testing.
func (p *PacketIO) WriteOKPacket(status uint16, header byte) error {
	data := make([]byte, 0, 7)
	data = append(data, header)
	data = append(data, 0, 0)
	// ClientProtocol41 must be enabled.
	data = DumpUint16(data, status)
	data = append(data, 0, 0)
	return p.WritePacket(data, true)
}

// WriteEOFPacket writes an EOF packet. It's only for testing.
func (p *PacketIO) WriteEOFPacket(status uint16) error {
	data := make([]byte, 0, 5)
	data = append(data, mysql.EOFHeader)
	data = append(data, 0, 0)
	// ClientProtocol41 must be enabled.
	data = DumpUint16(data, status)
	return p.WritePacket(data, true)
}
