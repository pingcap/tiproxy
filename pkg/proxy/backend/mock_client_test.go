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

	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
)

type clientConfig struct {
	// for auth
	tlsConfig  *tls.Config
	capability uint32
	username   string
	dbName     string
	collation  uint8
	authPlugin string
	authData   []byte
	attrs      []byte
	// for cmd
	cmd      byte
	filePkts int
}

type mockClient struct {
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
	if _, err := packetIO.ReadPacket(); err != nil {
		return err
	}

	var resp []byte
	var headerPos int
	if mc.capability&mysql.ClientProtocol41 > 0 {
		resp, headerPos = pnet.MakeNewVersionHandshakeResponse(mc.username, mc.dbName, mc.authPlugin, mc.collation, mc.authData, mc.attrs, mc.capability)
	} else {
		resp, headerPos = pnet.MakeOldVersionHandshakeResponse(mc.username, mc.dbName, mc.authData, mc.capability)
	}
	if mc.capability&mysql.ClientSSL > 0 {
		if err := packetIO.WritePacket(resp[:headerPos], true); err != nil {
			return err
		}
		if err := packetIO.UpgradeToClientTLS(mc.tlsConfig); err != nil {
			return err
		}
	}
	if err := packetIO.WritePacket(resp, true); err != nil {
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
	data := []byte{mc.cmd}
	switch mc.cmd {
	case mysql.ComInitDB, mysql.ComCreateDB, mysql.ComDropDB:
		data = append(data, []byte(mockCmdStr)...)
	case mysql.ComQuery:
		return mc.query(packetIO)
	case mysql.ComFieldList:
		data = append(data, []byte(mockCmdStr)...)
		data = append(data, 0x00)
		data = append(data, []byte(mockCmdStr)...)
	case mysql.ComRefresh, mysql.ComSetOption:
		data = append(data, mockCmdByte)
	case mysql.ComProcessKill:
		data = append(data, byte(mockCmdInt), byte(mockCmdInt>>8), byte(mockCmdInt>>16), byte(mockCmdInt>>24))
	case mysql.ComChangeUser:
		return mc.requestChangeUser(packetIO)
	}
	if err := packetIO.WritePacket(data, true); err != nil {
		return err
	}
	switch mc.cmd {
	case mysql.ComQuit:
		return nil
	}
	_, err := packetIO.ReadPacket()
	return err
}

func (mc *mockClient) requestChangeUser(packetIO *pnet.PacketIO) error {
	data := pnet.MakeChangeUser(mockUsername, mockDBName, mockAuthData)
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

func (mc *mockClient) query(packetIO *pnet.PacketIO) error {
	data := make([]byte, 0, len(mockCmdStr)+1)
	data = append(data, mysql.ComQuery)
	data = append(data, []byte(mockCmdStr)...)
	if err := packetIO.WritePacket(data, true); err != nil {
		return err
	}
	pkt, err := packetIO.ReadPacket()
	if err != nil {
		return err
	}
	switch pkt[0] {
	case mysql.OKHeader:
		// check status
	case mysql.ErrHeader:
		return nil
	case mysql.LocalInFileHeader:
		for i := 0; i < mc.filePkts; i++ {
			if err = packetIO.WritePacket(mockCmdBytes, false); err != nil {
				return err
			}
		}
		if err = packetIO.WritePacket(nil, true); err != nil {
			return err
		}
		if _, err = packetIO.ReadPacket(); err != nil {
			return err
		}
	default:
		// read result set
		for {
			if pkt, err = packetIO.ReadPacket(); err != nil {
				return err
			}
			if pnet.IsEOFPacket(pkt) {
				break
			}
		}
		for {
			if pkt, err = packetIO.ReadPacket(); err != nil {
				return err
			}
			if pkt[0] == mysql.ErrHeader {
				return nil
			}
			if pnet.IsEOFPacket(pkt) {
				break
			}
		}
	}
	return nil
}
