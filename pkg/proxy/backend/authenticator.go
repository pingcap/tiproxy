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
	"go.uber.org/zap"
)

var (
	ErrCapabilityNegotiation = errors.New("capability negotiation failed")
)

// Other server capabilities are not supported.
const requiredCapabilities = pnet.ClientProtocol41
const supportedServerCapabilities = pnet.ClientLongPassword | pnet.ClientFoundRows | pnet.ClientLongFlag |
	pnet.ClientConnectWithDB | pnet.ClientNoSchema | pnet.ClientODBC | pnet.ClientLocalFiles | pnet.ClientIgnoreSpace |
	pnet.ClientInteractive | pnet.ClientSSL | pnet.ClientIgnoreSigpipe |
	pnet.ClientTransactions | pnet.ClientReserved | pnet.ClientSecureConnection | pnet.ClientMultiStatements |
	pnet.ClientMultiResults | pnet.ClientPluginAuth | pnet.ClientConnectAttrs | pnet.ClientPluginAuthLenencClientData |
	pnet.ClientDeprecateEOF | requiredCapabilities

// Authenticator handshakes with the client and the backend.
type Authenticator struct {
	backendTLSConfig            *tls.Config
	supportedServerCapabilities pnet.Capability
	dbname                      string // default database name
	serverAddr                  string
	user                        string
	attrs                       []byte // no need to parse
	capability                  uint32 // client capability
	collation                   uint8
}

func (auth *Authenticator) String() string {
	return fmt.Sprintf("user:%s, dbname:%s, capability:%d, collation:%d",
		auth.user, auth.dbname, auth.capability, auth.collation)
}

func (auth *Authenticator) handshakeFirstTime(logger *zap.Logger, clientIO, backendIO *pnet.PacketIO, salt []byte, frontendTLSConfig, backendTLSConfig *tls.Config, proxyProtocol bool) error {
	clientIO.ResetSequence()
	backendIO.ResetSequence()

	proxyCapability := auth.supportedServerCapabilities
	if frontendTLSConfig == nil {
		proxyCapability ^= pnet.ClientSSL
	}
	if err := clientIO.WriteInitialHandshake(proxyCapability.Uint32(), salt, mysql.AuthCachingSha2Password); err != nil {
		return errors.WithStack(err)
	}
	pkt, isSSL, err := clientIO.ReadSSLRequestOrHandshakeResp()
	if err != nil {
		return errors.WithStack(err)
	}
	frontendCapability := pnet.Capability(binary.LittleEndian.Uint32(pkt))
	if isSSL {
		if proxyCapability&pnet.ClientSSL == 0 {
			return clientIO.WriteErrPacket(mysql.NewErr(mysql.ErrAbortingConnection, "should not go here", nil))
		}
		if _, err = clientIO.UpgradeToServerTLS(frontendTLSConfig); err != nil {
			return errors.WithStack(err)
		}
		pkt, isSSL, err = clientIO.ReadSSLRequestOrHandshakeResp()
		if err != nil {
			return errors.WithStack(err)
		}
		if isSSL {
			return errors.WithStack(errors.New("expect handshake resp"))
		}
	} else {
		binary.LittleEndian.PutUint32(pkt, (frontendCapability | pnet.ClientSSL).Uint32())
	}
	if commonCaps := frontendCapability & requiredCapabilities; commonCaps != requiredCapabilities {
		logger.Error("require frontend capabilities", zap.Stringer("common", commonCaps), zap.Stringer("required", requiredCapabilities))
		return errors.Wrapf(ErrCapabilityNegotiation, "require %s", requiredCapabilities&^commonCaps)
	}
	commonCaps := frontendCapability & proxyCapability
	if frontendCapability^commonCaps != 0 {
		logger.Error("frontend send capabilities unsupported by proxy", zap.Stringer("common", commonCaps), zap.Stringer("frontend", frontendCapability^commonCaps), zap.Stringer("proxy", proxyCapability^commonCaps))
		return errors.Wrapf(ErrCapabilityNegotiation, "%s", frontendCapability^commonCaps)
	}
	resp := pnet.ParseHandshakeResponse(pkt)
	auth.capability = commonCaps.Uint32()
	auth.user = resp.User
	auth.dbname = resp.DB
	auth.collation = resp.Collation
	auth.attrs = resp.Attrs

	// write proxy header
	if proxyProtocol {
		proxy := clientIO.Proxy()
		if proxy == nil {
			proxy = &pnet.Proxy{
				SrcAddress: clientIO.RemoteAddr(),
				DstAddress: backendIO.RemoteAddr(),
				Version:    pnet.ProxyVersion2,
				Command:    pnet.ProxyCommandProxy,
			}
		}
		if err := backendIO.WriteProxyV2(proxy); err != nil {
			return err
		}
	}

	// read server initial handshake
	_, backendCapabilityU, err := auth.readInitialHandshake(backendIO)
	if err != nil {
		return errors.WithStack(err)
	}
	backendCapability := pnet.Capability(backendCapabilityU)
	if common := commonCaps & backendCapability; (commonCaps^common)&^pnet.ClientSSL != 0 {
		logger.Info("backend does not support capabilities from client&proxy", zap.Stringer("common", common), zap.Stringer("server", commonCaps^common), zap.Stringer("backend", backendCapability^common))
	}
	if backendCapability&pnet.ClientSSL == 0 {
		// The error cannot be sent to the client because the client only expects an initial handshake packet.
		// The only way is to log it and disconnect.
		return errors.New("the TiDB server must enable TLS")
	}

	// write SSL Packet
	if err := backendIO.WritePacket(pkt[:32], true); err != nil {
		return errors.WithStack(err)
	}
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
		return errors.WithStack(err)
	}

	// forward client handshake resp
	if err := backendIO.WritePacket(pkt, true); err != nil {
		return errors.WithStack(err)
	}

	// forward other packets
	for {
		serverPkt, err := forwardMsg(backendIO, clientIO)
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
	data := pnet.MakeHandshakeResponse(auth.user, auth.dbname, mysql.AuthTiDBSessionToken,
		auth.collation, authData, auth.attrs, capability)

	// write SSL req
	if err := backendIO.WritePacket(data[:32], true); err != nil {
		return err
	}
	// Send TLS / SSL request packet. The server must have supported TLS.
	if err := backendIO.UpgradeToClientTLS(auth.backendTLSConfig); err != nil {
		return err
	}
	// write handshake resp
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
