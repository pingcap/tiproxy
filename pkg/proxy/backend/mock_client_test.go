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

	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
)

type clientConfig struct {
	// for auth
	tlsConfig  *tls.Config
	sql        string
	username   string
	dbName     string
	authPlugin string
	attrs      map[string]string
	dataBytes  []byte
	authData   []byte
	filePkts   int
	prepStmtID int
	capability pnet.Capability
	collation  uint8
	// for cmd
	cmd byte
	// for both auth and cmd
	abnormalExit bool
}

func newClientConfig() *clientConfig {
	return &clientConfig{
		capability: defaultTestClientCapability,
		username:   mockUsername,
		dbName:     mockDBName,
		authPlugin: mysql.AuthCachingSha2Password,
		authData:   mockAuthData,
		attrs:      make(map[string]string),
		cmd:        mysql.ComQuery,
		dataBytes:  mockCmdBytes,
		sql:        mockCmdStr,
	}
}

type mockClient struct {
	err error
	// Inputs that assigned by the test and will be sent to the server.
	*clientConfig
	// Outputs that received from the server and will be checked by the test.
	authSucceed bool
}

func newMockClient(cfg *clientConfig) *mockClient {
	return &mockClient{
		clientConfig: cfg,
	}
}

func (mc *mockClient) authenticate(packetIO *pnet.PacketIO) error {
	if mc.abnormalExit {
		return packetIO.Close()
	}
	pkt, err := packetIO.ReadPacket()
	if err != nil {
		return err
	}
	serverCap := pnet.ParseInitialHandshake(pkt)
	mc.capability = mc.capability & serverCap

	resp := &pnet.HandshakeResp{
		User:       mc.username,
		DB:         mc.dbName,
		AuthPlugin: mc.authPlugin,
		Attrs:      mc.attrs,
		AuthData:   mc.authData,
		Capability: mc.capability.Uint32(),
		Collation:  mc.collation,
	}
	pkt = pnet.MakeHandshakeResponse(resp)
	if mc.capability&pnet.ClientSSL > 0 {
		if err := packetIO.WritePacket(pkt[:32], true); err != nil {
			return err
		}
		if err := packetIO.ClientTLSHandshake(mc.tlsConfig); err != nil {
			return err
		}
	}
	if err := packetIO.WritePacket(pkt, true); err != nil {
		return err
	}
	return mc.writePassword(packetIO)
}

func (mc *mockClient) writePassword(packetIO *pnet.PacketIO) error {
	for {
		serverPkt, err := packetIO.ReadPacket()
		if err != nil {
			return err
		}
		switch serverPkt[0] {
		case mysql.OKHeader:
			mc.authSucceed = true
			return nil
		case mysql.ErrHeader:
			mc.authSucceed = false
			return nil
		case mysql.AuthSwitchRequest, pnet.ShaCommand:
			if err := packetIO.WritePacket(mc.authData, true); err != nil {
				return err
			}
		}
	}
}

// request sends commands except prepared statements commands.
func (mc *mockClient) request(packetIO *pnet.PacketIO) error {
	if mc.abnormalExit {
		return packetIO.Close()
	}
	packetIO.ResetSequence()
	data := []byte{mc.cmd}
	switch mc.cmd {
	case mysql.ComInitDB, mysql.ComCreateDB, mysql.ComDropDB:
		data = append(data, []byte(mockCmdStr)...)
	case mysql.ComQuery:
		return mc.query(packetIO)
	case mysql.ComProcessInfo:
		return mc.requestProcessInfo(packetIO)
	case mysql.ComFieldList:
		return mc.requestFieldList(packetIO)
	case mysql.ComRefresh, mysql.ComSetOption:
		data = append(data, mc.dataBytes...)
	case mysql.ComProcessKill:
		data = pnet.DumpUint32(data, uint32(mockCmdInt))
	case mysql.ComChangeUser:
		return mc.requestChangeUser(packetIO)
	case mysql.ComStmtPrepare:
		return mc.requestPrepare(packetIO)
	case mysql.ComStmtSendLongData:
		data = pnet.DumpUint32(data, uint32(mc.prepStmtID))
		data = append(data, mc.dataBytes...)
	case mysql.ComStmtExecute:
		return mc.requestExecute(packetIO)
	case mysql.ComStmtFetch:
		return mc.requestFetch(packetIO)
	case mysql.ComStmtClose, mysql.ComStmtReset:
		data = pnet.DumpUint32(data, uint32(mc.prepStmtID))
	}
	if err := packetIO.WritePacket(data, true); err != nil {
		return err
	}
	switch mc.cmd {
	case mysql.ComQuit, mysql.ComStmtClose, mysql.ComStmtSendLongData:
		return nil
	}
	_, err := packetIO.ReadPacket()
	return err
}

func (mc *mockClient) requestChangeUser(packetIO *pnet.PacketIO) error {
	data := pnet.MakeChangeUser(mc.username, mc.dbName, mysql.AuthNativePassword, mc.authData)
	if err := packetIO.WritePacket(data, true); err != nil {
		return err
	}
	for {
		resp, err := packetIO.ReadPacket()
		if err != nil {
			return err
		}
		switch resp[0] {
		case mysql.OKHeader, mysql.ErrHeader:
			return nil
		default:
			if err := packetIO.WritePacket(mc.authData, true); err != nil {
				return err
			}
		}
	}
}

func (mc *mockClient) requestPrepare(packetIO *pnet.PacketIO) error {
	data := make([]byte, 0, len(mc.sql)+1)
	data = append(data, mysql.ComStmtPrepare)
	data = append(data, []byte(mc.sql)...)
	if err := packetIO.WritePacket(data, true); err != nil {
		return err
	}
	response, err := packetIO.ReadPacket()
	if err != nil {
		return err
	}
	expectedPacketNum := 0
	if response[0] == mysql.OKHeader {
		numColumns := binary.LittleEndian.Uint16(response[5:])
		numParams := binary.LittleEndian.Uint16(response[7:])
		expectedPacketNum = int(numColumns) + int(numParams)
		if mc.capability&pnet.ClientDeprecateEOF == 0 {
			if numColumns > 0 {
				expectedPacketNum++
			}
			if numParams > 0 {
				expectedPacketNum++
			}
		}
	}
	for i := 0; i < expectedPacketNum; i++ {
		if _, err = packetIO.ReadPacket(); err != nil {
			return err
		}
	}
	return nil
}

func (mc *mockClient) requestExecute(packetIO *pnet.PacketIO) error {
	data := make([]byte, 0, len(mc.dataBytes)+5)
	data = append(data, mysql.ComStmtExecute)
	data = pnet.DumpUint32(data, uint32(mc.prepStmtID))
	data = append(data, mc.dataBytes...)
	if err := packetIO.WritePacket(data, true); err != nil {
		return err
	}
	return mc.readResultSet(packetIO)
}

func (mc *mockClient) requestFetch(packetIO *pnet.PacketIO) error {
	data := make([]byte, 0, len(mc.dataBytes)+5)
	data = append(data, mysql.ComStmtFetch)
	data = pnet.DumpUint32(data, uint32(mc.prepStmtID))
	data = append(data, mc.dataBytes...)
	if err := packetIO.WritePacket(data, true); err != nil {
		return err
	}
	_, err := mc.readUntilResultEnd(packetIO)
	return err
}

func (mc *mockClient) requestFieldList(packetIO *pnet.PacketIO) error {
	data := make([]byte, 0, len(mockCmdStr)+2)
	data = append(data, mysql.ComFieldList)
	data = append(data, []byte(mockCmdStr)...)
	data = append(data, 0x00)
	data = append(data, []byte(mockCmdStr)...)
	if err := packetIO.WritePacket(data, true); err != nil {
		return err
	}
	_, err := mc.readUntilResultEnd(packetIO)
	return err
}

func (mc *mockClient) readUntilResultEnd(packetIO *pnet.PacketIO) (pkt []byte, err error) {
	for {
		pkt, err = packetIO.ReadPacket()
		if err != nil {
			return
		}
		if pkt[0] == mysql.ErrHeader {
			return
		}
		if mc.capability&pnet.ClientDeprecateEOF == 0 {
			if pnet.IsEOFPacket(pkt) {
				break
			}
		} else {
			if pnet.IsResultSetOKPacket(pkt) {
				break
			}
		}
	}
	return
}

func (mc *mockClient) requestProcessInfo(packetIO *pnet.PacketIO) error {
	if err := packetIO.WritePacket([]byte{mysql.ComProcessInfo}, true); err != nil {
		return err
	}
	return mc.readResultSet(packetIO)
}

func (mc *mockClient) query(packetIO *pnet.PacketIO) error {
	data := make([]byte, 0, len(mc.sql)+1)
	data = append(data, mysql.ComQuery)
	data = append(data, []byte(mc.sql)...)
	if err := packetIO.WritePacket(data, true); err != nil {
		return err
	}
	return mc.readResultSet(packetIO)
}

func (mc *mockClient) readResultSet(packetIO *pnet.PacketIO) error {
	for {
		var serverStatus uint16
		pkt, err := packetIO.ReadPacket()
		if err != nil {
			return err
		}
		switch pkt[0] {
		case mysql.OKHeader:
			serverStatus = binary.LittleEndian.Uint16(pkt[3:])
		case mysql.ErrHeader:
			return nil
		case mysql.LocalInFileHeader:
			for i := 0; i < mc.filePkts; i++ {
				if err = packetIO.WritePacket(mc.dataBytes, false); err != nil {
					return err
				}
			}
			if err = packetIO.WritePacket(nil, true); err != nil {
				return err
			}
			if pkt, err = packetIO.ReadPacket(); err != nil {
				return err
			}
			if pkt[0] == mysql.OKHeader {
				serverStatus = binary.LittleEndian.Uint16(pkt[3:])
			} else {
				return nil
			}
		default:
			// read result set
			if mc.capability&pnet.ClientDeprecateEOF == 0 {
				if pkt, err = mc.readUntilResultEnd(packetIO); err != nil {
					return err
				}
				if pkt[0] == mysql.ErrHeader {
					return nil
				}
				serverStatus = binary.LittleEndian.Uint16(pkt[3:])
				if serverStatus&mysql.ServerStatusCursorExists > 0 {
					break
				}
			}
			if pkt, err = mc.readUntilResultEnd(packetIO); err != nil {
				return err
			}
			if pkt[0] == mysql.ErrHeader {
				return nil
			}
			if mc.capability&pnet.ClientDeprecateEOF == 0 {
				serverStatus = binary.LittleEndian.Uint16(pkt[3:])
			} else {
				rs := pnet.ParseOKPacket(pkt)
				serverStatus = rs.Status
			}
		}
		if serverStatus&mysql.ServerMoreResultsExists == 0 {
			break
		}
	}
	return nil
}
