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

	"github.com/pingcap/TiProxy/pkg/util/errors"
	"github.com/pingcap/tidb/parser/mysql"
)

func (p *PacketIO) WriteInitialHandshake(capability uint32, status uint16, salt []byte) error {
	data := make([]byte, 0, 128)

	// min version 10
	data = append(data, 10)
	// server version[NUL]
	data = append(data, serverVersion...)
	data = append(data, 0)
	// connection id
	data = append(data, byte(capability), byte(capability>>8), byte(capability>>16), byte(capability>>24))
	// auth-plugin-data-part-1
	data = append(data, salt[:]...)
	// filler [00]
	data = append(data, 0)
	// capability flag lower 2 bytes, using default capability here
	data = append(data, byte(capability), byte(capability>>8))
	// charset
	data = append(data, utf8mb4BinID)
	// status
	data = append(data, byte(status), byte(status>>8))
	// capability flag upper 2 bytes, using default capability here
	data = append(data, byte(capability>>16), byte(capability>>24))
	// length of auth-plugin-data
	data = append(data, byte(len(salt)+1))
	// reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	// auth-plugin-data-part-2
	data = append(data, salt[8:]...)
	data = append(data, 0)
	// auth-plugin name
	data = append(data, []byte("mysql_native_password")...)
	data = append(data, 0)

	if err := p.WritePacket(data, true); err != nil {
		return err
	}

	return p.Flush()
}

func (p *PacketIO) ReadSSLRequest() ([]byte, error) {
	pkt, err := p.ReadPacket()
	if err != nil {
		return nil, err
	}

	if len(pkt) < 2 {
		return nil, errors.WithStack(errors.Errorf("%w: but got less than 2 bytes", ErrExpectSSLRequest))
	}

	capability := uint32(binary.LittleEndian.Uint16(pkt[:2]))
	if capability&mysql.ClientSSL == 0 {
		return nil, errors.WithStack(errors.Errorf("%w: but capability flags has no SSL", ErrExpectSSLRequest))
	}

	return pkt, nil
}

func (p *PacketIO) WriteErrPacket(code uint16, msg string) error {
	data := make([]byte, 0, 64)
	data = append(data, 0xff)
	data = append(data, byte(code), byte(code>>8))
	// fill spaces for 421 protocol for compatibility
	data = append(data, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20)
	data = append(data, msg...)

	if err := p.WritePacket(data, true); err != nil {
		return err
	}

	return p.Flush()
}
