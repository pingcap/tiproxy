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
	"fmt"

	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Authenticator handshakes with the client and the backend.
type Authenticator struct {
	user             string
	dbname           string // default database name
	capability       uint32 // client capability
	collation        uint8
	attrs            []byte // no need to parse
	backendTLSConfig *tls.Config
}

func (auth *Authenticator) String() string {
	return fmt.Sprintf("user:%s, dbname:%s, capability:%d, collation:%d",
		auth.user, auth.dbname, auth.capability, auth.collation)
}

func (auth *Authenticator) handshakeFirstTime(clientIO, backendIO *pnet.PacketIO, serverTLSConfig, backendTLSConfig *tls.Config) (bool, error) {
	backendIO.ResetSequence()
	var (
		serverPkt, clientPkt []byte
		err                  error
		serverCapability     uint32
	)

	// Read initial handshake packet from the backend.
	serverPkt, serverCapability, err = auth.readInitialHandshake(backendIO)
	if serverPkt != nil {
		writeErr := clientIO.WritePacket(serverPkt, true)
		if writeErr != nil {
			return false, writeErr
		}
		if err != nil {
			return false, nil
		}
	} else {
		return false, err
	}
	if serverCapability&mysql.ClientSSL == 0 {
		return false, errors.New("the TiDB server must enable TLS")
	}

	// Read the response from the client.
	if clientPkt, err = clientIO.ReadPacket(); err != nil {
		return false, err
	}
	capability := binary.LittleEndian.Uint16(clientPkt[:2])
	// A 2-bytes capability contains the ClientSSL flag, no matter ClientProtocol41 is set or not.
	sslEnabled := uint32(capability)&mysql.ClientSSL > 0
	if sslEnabled {
		// Upgrade TLS with the client if SSL is enabled.
		if _, err = clientIO.UpgradeToServerTLS(serverTLSConfig); err != nil {
			return false, err
		}
	} else {
		// Rewrite the packet with ClientSSL enabled because we always connect to TiDB with TLS.
		pktWithSSL := make([]byte, len(clientPkt))
		pnet.DumpUint16(pktWithSSL[:0], capability|uint16(mysql.ClientSSL))
		copy(pktWithSSL[2:], clientPkt[2:])
		clientPkt = pktWithSSL
	}
	if err = backendIO.WritePacket(clientPkt, true); err != nil {
		return false, err
	}
	// Always upgrade TLS with the server.
	auth.backendTLSConfig = backendTLSConfig
	if err = backendIO.UpgradeToClientTLS(backendTLSConfig); err != nil {
		return false, err
	}
	if sslEnabled {
		// Read from the client again, where the capability may not contain ClientSSL this time.
		if clientPkt, err = clientIO.ReadPacket(); err != nil {
			return false, err
		}
	}
	// Send the response again.
	if err = backendIO.WritePacket(clientPkt, true); err != nil {
		return false, err
	}
	auth.readHandshakeResponse(clientPkt)

	// verify password
	for {
		serverPkt, err = forwardMsg(backendIO, clientIO)
		if err != nil {
			return false, err
		}
		switch serverPkt[0] {
		case mysql.OKHeader:
			logutil.BgLogger().Debug("parse client handshake response finished", zap.String("authInfo", auth.String()))
			return true, nil
		case mysql.ErrHeader:
			return false, nil
		default: // mysql.AuthSwitchRequest, ShaCommand
			clientPkt, err = forwardMsg(clientIO, backendIO)
			if err != nil {
				return false, err
			}
		}
	}
}

func forwardMsg(srcIO, destIO *pnet.PacketIO) (data []byte, err error) {
	data, err = srcIO.ReadPacket()
	if err != nil {
		return
	}
	err = destIO.WritePacket(data, true)
	return
}

func (auth *Authenticator) readHandshakeResponse(data []byte) {
	resp := pnet.ParseHandshakeResponse(data)
	auth.capability = resp.Capability
	auth.user = resp.User
	auth.dbname = resp.DB
	auth.collation = resp.Collation
	auth.attrs = resp.Attrs
}

func (auth *Authenticator) handshakeSecondTime(backendIO *pnet.PacketIO, sessionToken string) error {
	if len(sessionToken) == 0 {
		return errors.New("session token is empty")
	}

	_, serverCapability, err := auth.readInitialHandshake(backendIO)
	if err != nil {
		return err
	}
	if serverCapability&mysql.ClientSSL == 0 {
		return errors.New("the TiDB server must enable TLS")
	}

	tokenBytes := hack.Slice(sessionToken)
	err = auth.writeAuthHandshake(backendIO, tokenBytes)
	if err != nil {
		return err
	}

	return auth.handleSecondAuthResult(backendIO)
}

func (auth *Authenticator) readInitialHandshake(backendIO *pnet.PacketIO) (serverPkt []byte, capability uint32, err error) {
	if serverPkt, err = backendIO.ReadPacket(); err != nil {
		err = errors.Trace(err)
		return
	}
	if serverPkt[0] == mysql.ErrHeader {
		err = errors.New("read initial handshake error")
		return
	}
	capability = pnet.ParseInitialHandshake(serverPkt)
	return
}

func (auth *Authenticator) writeAuthHandshake(backendIO *pnet.PacketIO, authData []byte) error {
	// Always handshake with SSL enabled.
	capability := auth.capability | mysql.ClientSSL
	// Always enable auth_plugin.
	capability |= mysql.ClientPluginAuth
	// Always use the new version.
	data, headerPos := pnet.GetNewVersionHandshakeResponse(auth.user, auth.dbname, mysql.AuthTiDBSessionToken,
		auth.collation, authData, auth.attrs, capability)

	// write header
	if err := backendIO.WritePacket(data[:headerPos], true); err != nil {
		return err
	}
	// Send TLS / SSL request packet. The server must have supported TLS.
	if err := backendIO.UpgradeToClientTLS(auth.backendTLSConfig); err != nil {
		return err
	}
	// write body
	return backendIO.WritePacket(data, true)
}

func (auth *Authenticator) handleSecondAuthResult(backendIO *pnet.PacketIO) error {
	data, err := backendIO.ReadPacket()
	if err != nil {
		return err
	}

	switch data[0] {
	case mysql.OKHeader:
		return nil
	case mysql.ErrHeader:
		return errors.New("auth failed")
	default: // mysql.AuthSwitchRequest, ShaCommand:
		return errors.Errorf("read unexpected command: %#x", data[0])
	}
}

// changeUser is called once the client sends COM_CHANGE_USER.
func (auth *Authenticator) changeUser(username, db string) {
	auth.user = username
	auth.dbname = db
	// TODO: attrs
}

// updateCurrentDB is called once the client sends COM_INIT_DB or `use db`.
// The proxy cannot send the original dbname to TiDB in the second handshake because the original db may be dropped.
func (auth *Authenticator) updateCurrentDB(db string) {
	auth.dbname = db
}
