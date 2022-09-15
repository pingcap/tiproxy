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
	"net"

	"github.com/pingcap/TiProxy/lib/util/errors"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/hack"
)

// Other server capabilities are not supported.
const supportedServerCapabilities = mysql.ClientLongPassword | mysql.ClientFoundRows | mysql.ClientLongFlag |
	mysql.ClientConnectWithDB | mysql.ClientNoSchema | mysql.ClientODBC | mysql.ClientLocalFiles | mysql.ClientIgnoreSpace |
	mysql.ClientProtocol41 | mysql.ClientInteractive | mysql.ClientSSL | mysql.ClientIgnoreSigpipe |
	mysql.ClientTransactions | mysql.ClientReserved | mysql.ClientSecureConnection | mysql.ClientMultiStatements |
	mysql.ClientMultiResults | mysql.ClientPluginAuth | mysql.ClientConnectAtts | mysql.ClientPluginAuthLenencClientData |
	mysql.ClientDeprecateEOF

// Authenticator handshakes with the client and the backend.
type Authenticator struct {
	user             string
	dbname           string // default database name
	capability       uint32 // client capability
	collation        uint8
	serverAddr       string
	attrs            []byte // no need to parse
	backendTLSConfig *tls.Config
}

func (auth *Authenticator) String() string {
	return fmt.Sprintf("user:%s, dbname:%s, capability:%d, collation:%d",
		auth.user, auth.dbname, auth.capability, auth.collation)
}

func (auth *Authenticator) handshakeFirstTime(clientIO, backendIO *pnet.PacketIO, frontendTLSConfig, backendTLSConfig *tls.Config) error {
	backendIO.ResetSequence()
	// Read initial handshake packet from the backend.
	serverPkt, serverCapability, err := auth.readInitialHandshake(backendIO)
	if serverPkt != nil {
		if writeErr := clientIO.WritePacket(serverPkt, true); writeErr != nil {
			return writeErr
		}
	}
	if err != nil {
		return err
	}
	if serverCapability&mysql.ClientSSL == 0 {
		// The error cannot be sent to the client because the client only expects an initial handshake packet.
		// The only way is to log it and disconnect.
		return errors.New("the TiDB server must enable TLS")
	}

	// Read the response from the client.
	clientPkt, err := clientIO.ReadPacket()
	if err != nil {
		return err
	}
	clientCapability := binary.LittleEndian.Uint16(clientPkt[:2])
	// A 2-bytes capability contains the ClientSSL flag, no matter ClientProtocol41 is set or not.
	sslEnabled := uint32(clientCapability)&mysql.ClientSSL > 0
	if sslEnabled {
		// Upgrade TLS with the client if SSL is enabled.
		if _, err = clientIO.UpgradeToServerTLS(frontendTLSConfig); err != nil {
			return err
		}
	} else {
		// Rewrite the packet with ClientSSL enabled because we always connect to TiDB with TLS.
		pktWithSSL := make([]byte, len(clientPkt))
		pnet.DumpUint16(pktWithSSL[:0], clientCapability|uint16(mysql.ClientSSL))
		copy(pktWithSSL[2:], clientPkt[2:])
		clientPkt = pktWithSSL
	}
	if err = backendIO.WritePacket(clientPkt, true); err != nil {
		return err
	}
	// Always upgrade TLS with the server.
	auth.backendTLSConfig = backendTLSConfig.Clone()
	addr := backendIO.RemoteAddr().String()
	if auth.serverAddr != "" {
		// NOTE: should use DNS name as much as possible
		// Usally certs are signed with domain instead of IP addrs
		// And `RemoteAddr()` will return IP addr
		addr = auth.serverAddr
	}
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		auth.backendTLSConfig.ServerName = host
	}
	if err = backendIO.UpgradeToClientTLS(auth.backendTLSConfig); err != nil {
		return err
	}
	if sslEnabled {
		// Read from the client again, where the capability may not contain ClientSSL this time.
		if clientPkt, err = clientIO.ReadPacket(); err != nil {
			return err
		}
	}
	// Send the response again.
	if err = backendIO.WritePacket(clientPkt, true); err != nil {
		return err
	}
	if err = auth.readHandshakeResponse(clientPkt, serverCapability); err != nil {
		return err
	}

	// verify password
	for {
		serverPkt, err = forwardMsg(backendIO, clientIO)
		if err != nil {
			return err
		}
		switch serverPkt[0] {
		case mysql.OKHeader:
			return nil
		case mysql.ErrHeader:
			return pnet.ParseErrorPacket(serverPkt)
		default: // mysql.AuthSwitchRequest, ShaCommand
			if _, err = forwardMsg(clientIO, backendIO); err != nil {
				return err
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

func (auth *Authenticator) readHandshakeResponse(data []byte, serverCapability uint32) error {
	capability := uint32(binary.LittleEndian.Uint16(data[:2]))
	if capability&mysql.ClientProtocol41 == 0 {
		// TiDB doesn't support it now.
		return errors.New("pre-4.1 MySQL client versions are not supported")
	}
	resp := pnet.ParseHandshakeResponse(data)
	auth.capability = resp.Capability & serverCapability
	if unsupported := auth.capability &^ supportedServerCapabilities; unsupported > 0 {
		return errors.Errorf("capability is not supported: %d", unsupported)
	}
	auth.user = resp.User
	auth.dbname = resp.DB
	auth.collation = resp.Collation
	auth.attrs = resp.Attrs
	return nil
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
	if err = auth.writeAuthHandshake(backendIO, tokenBytes); err != nil {
		return err
	}

	return auth.handleSecondAuthResult(backendIO)
}

func (auth *Authenticator) readInitialHandshake(backendIO *pnet.PacketIO) (serverPkt []byte, capability uint32, err error) {
	if serverPkt, err = backendIO.ReadPacket(); err != nil {
		return
	}
	if pnet.IsErrorPacket(serverPkt) {
		err = pnet.ParseErrorPacket(serverPkt)
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
	data, headerPos := pnet.MakeHandshakeResponse(auth.user, auth.dbname, mysql.AuthTiDBSessionToken,
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
		return pnet.ParseErrorPacket(data)
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
