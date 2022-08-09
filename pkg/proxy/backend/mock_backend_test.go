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

package backend

import (
	"crypto/tls"
	"encoding/binary"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
)

type backendConfig struct {
	// for auth
	tlsConfig   *tls.Config
	capability  uint32
	salt        []byte
	authPlugin  string
	switchAuth  bool
	authSucceed bool
	// for cmd
	respondType respondType
	columns     int
	rows        int
	params      int
	status      uint16
}

type mockBackend struct {
	// Inputs that assigned by the test and will be sent to the client.
	*backendConfig
	// Outputs that received from the client and will be checked by the test.
	username string
	authData []byte
	db       string
	attrs    []byte
	err      error
}

func newMockBackend(cfg *backendConfig) *mockBackend {
	return &mockBackend{
		backendConfig: cfg,
	}
}

func (mb *mockBackend) authenticate(packetIO *pnet.PacketIO) error {
	var err error
	// write initial handshake
	if err = packetIO.WriteInitialHandshake(mb.capability, mb.salt, mb.authPlugin); err != nil {
		return err
	}
	// read the response
	var clientPkt []byte
	if clientPkt, err = packetIO.ReadPacket(); err != nil {
		return err
	}
	// upgrade to TLS
	capability := binary.LittleEndian.Uint16(clientPkt[:2])
	sslEnabled := uint32(capability)&mysql.ClientSSL > 0 && mb.capability&mysql.ClientSSL > 0
	if sslEnabled {
		if _, err = packetIO.UpgradeToServerTLS(mb.tlsConfig); err != nil {
			return err
		}
		// read the response again
		if clientPkt, err = packetIO.ReadPacket(); err != nil {
			return err
		}
	}
	resp := pnet.ParseHandshakeResponse(clientPkt)
	mb.username = resp.User
	mb.db = resp.DB
	mb.authData = resp.AuthData
	mb.attrs = resp.Attrs
	// verify password
	return mb.verifyPassword(packetIO)
}

func (mb *mockBackend) verifyPassword(packetIO *pnet.PacketIO) error {
	if mb.switchAuth {
		var err error
		if err = packetIO.WriteSwitchRequest(mb.authPlugin, mb.salt); err != nil {
			return err
		}
		if mb.authData, err = packetIO.ReadPacket(); err != nil {
			return err
		}
		switch mb.authPlugin {
		case mysql.AuthCachingSha2Password:
			if err = packetIO.WriteShaCommand(); err != nil {
				return err
			}
			if mb.authData, err = packetIO.ReadPacket(); err != nil {
				return err
			}
		}
	}
	if mb.authSucceed {
		if err := packetIO.WriteOKPacket(mb.status); err != nil {
			return err
		}
	} else {
		if err := packetIO.WriteErrPacket(mysql.NewErr(mysql.ErrAccessDenied)); err != nil {
			return err
		}
	}
	return nil
}

func (mb *mockBackend) respond(packetIO *pnet.PacketIO) error {
	packetIO.ResetSequence()
	if _, err := packetIO.ReadPacket(); err != nil {
		return err
	}
	switch mb.respondType {
	case responseTypeOK:
		return packetIO.WriteOKPacket(mb.status)
	case responseTypeErr:
		return packetIO.WriteErrPacket(mysql.NewErr(mysql.ErrUnknown))
	case responseTypeResultSet:
		return mb.respondResultSet(packetIO)
	case responseTypeColumn:
		return mb.respondColumns(packetIO)
	case responseTypeLoadFile:
		return mb.respondLoadFile(packetIO)
	case responseTypeString:
		return packetIO.WritePacket([]byte(mockCmdStr), true)
	case responseTypeEOF:
		return packetIO.WriteEOFPacket(mb.status)
	case responseTypeSwitchRequest:
		if err := packetIO.WriteSwitchRequest(mb.authPlugin, mb.salt); err != nil {
			return err
		}
		if _, err := packetIO.ReadPacket(); err != nil {
			return err
		}
		return packetIO.WriteOKPacket(mb.status)
	case responseTypePrepareOK:
		return mb.respondPrepare(packetIO)
	case responseTypeRow:
		return mb.respondRows(packetIO)
	case responseTypeNone:
		return nil
	}
	return packetIO.WriteErrPacket(mysql.NewErr(mysql.ErrUnknown))
}

// respond to FieldList
func (mb *mockBackend) respondColumns(packetIO *pnet.PacketIO) error {
	for i := 0; i < mb.columns; i++ {
		if err := packetIO.WritePacket(mockCmdBytes, false); err != nil {
			return err
		}
	}
	return packetIO.WriteEOFPacket(mb.status)
}

// respond to Fetch
func (mb *mockBackend) respondRows(packetIO *pnet.PacketIO) error {
	for i := 0; i < mb.rows; i++ {
		if err := packetIO.WritePacket(mockCmdBytes, false); err != nil {
			return err
		}
	}
	return packetIO.WriteEOFPacket(mb.status)
}

// respond to Query
func (mb *mockBackend) respondResultSet(packetIO *pnet.PacketIO) error {
	names := make([]string, 0, mb.columns)
	values := make([][]any, 0, mb.rows)
	for i := 0; i < mb.columns; i++ {
		names = append(names, mockCmdStr)
	}
	for i := 0; i < mb.rows; i++ {
		row := make([]any, 0, mb.columns)
		for j := 0; j < mb.columns; j++ {
			row = append(row, mockCmdStr)
		}
		values = append(values, row)
	}
	rs, err := gomysql.BuildSimpleTextResultset(names, values)
	if err != nil {
		return err
	}
	data := pnet.DumpLengthEncodedInt(nil, uint64(mb.columns))
	if err := packetIO.WritePacket(data, false); err != nil {
		return err
	}
	for _, field := range rs.Fields {
		if err := packetIO.WritePacket(field.Dump(), false); err != nil {
			return err
		}
	}
	if err := packetIO.WriteEOFPacket(mb.status); err != nil {
		return err
	}
	if mb.status&mysql.ServerStatusCursorExists == 0 {
		for _, row := range values {
			var data []byte
			for _, value := range row {
				data = pnet.DumpLengthEncodedString(data, []byte(value.(string)))
			}
			if err := packetIO.WritePacket(data, false); err != nil {
				return err
			}
		}
		return packetIO.WriteEOFPacket(mb.status)
	}
	return nil
}

// respond to LoadInFile
func (mb *mockBackend) respondLoadFile(packetIO *pnet.PacketIO) error {
	data := make([]byte, 0, 1+len(mockCmdStr))
	data = append(data, mysql.LocalInFileHeader)
	data = append(data, []byte(mockCmdStr)...)
	if err := packetIO.WritePacket(data, true); err != nil {
		return err
	}
	for {
		// read file data
		pkt, err := packetIO.ReadPacket()
		if err != nil {
			return err
		}
		// An empty packet indicates the end of file.
		if len(pkt) == 0 {
			break
		}
	}
	return packetIO.WriteOKPacket(mb.status)
}

// respond to Prepare
func (mb *mockBackend) respondPrepare(packetIO *pnet.PacketIO) error {
	data := []byte{mysql.OKHeader}
	data = pnet.DumpUint32(data, uint32(mockCmdInt))
	data = pnet.DumpUint16(data, uint16(mb.columns))
	data = pnet.DumpUint16(data, uint16(mb.params))
	data = append(data, 0x00)
	data = pnet.DumpUint16(data, uint16(mockCmdInt))
	if err := packetIO.WritePacket(data, true); err != nil {
		return err
	}
	if mb.params > 0 {
		for i := 0; i < mb.params; i++ {
			if err := packetIO.WritePacket(mockCmdBytes, false); err != nil {
				return err
			}
		}
		if err := packetIO.WriteEOFPacket(mb.status); err != nil {
			return err
		}
	}
	if mb.columns > 0 {
		for i := 0; i < mb.columns; i++ {
			if err := packetIO.WritePacket(mockCmdBytes, false); err != nil {
				return err
			}
		}
		if err := packetIO.WriteEOFPacket(mb.status); err != nil {
			return err
		}
	}
	return nil
}
