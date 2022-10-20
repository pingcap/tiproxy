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

const requiredFrontendCaps = pnet.ClientProtocol41
const defRequiredBackendCaps = pnet.ClientDeprecateEOF

// Other server capabilities are not supported. ClientDeprecateEOF is supported but TiDB 6.2.0 doesn't support it now.
const supportedServerCapabilities = pnet.ClientLongPassword | pnet.ClientFoundRows | pnet.ClientConnectWithDB |
	pnet.ClientODBC | pnet.ClientLocalFiles | pnet.ClientInteractive | pnet.ClientLongFlag | pnet.ClientSSL |
	pnet.ClientTransactions | pnet.ClientReserved | pnet.ClientSecureConnection | pnet.ClientMultiStatements |
	pnet.ClientMultiResults | pnet.ClientPluginAuth | pnet.ClientConnectAttrs | pnet.ClientPluginAuthLenencClientData |
	requiredFrontendCaps | defRequiredBackendCaps

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
	proxyProtocol               bool
	requireBackendTLS           bool
}

func (auth *Authenticator) String() string {
	return fmt.Sprintf("user:%s, dbname:%s, capability:%d, collation:%d",
		auth.user, auth.dbname, auth.capability, auth.collation)
}

func (auth *Authenticator) writeProxyProtocol(clientIO, backendIO *pnet.PacketIO) error {
	if auth.proxyProtocol {
		proxy := clientIO.Proxy()
		if proxy == nil {
			proxy = &pnet.Proxy{
				SrcAddress: clientIO.RemoteAddr(),
				DstAddress: backendIO.RemoteAddr(),
				Version:    pnet.ProxyVersion2,
			}
		}
		// either from another proxy or directly from clients, we are actings as a proxy
		proxy.Command = pnet.ProxyCommandProxy
		if err := backendIO.WriteProxyV2(proxy); err != nil {
			return err
		}
	}
	return nil
}

func (auth *Authenticator) handshakeFirstTime(logger *zap.Logger, clientIO *pnet.PacketIO, getBackendIO func(*Authenticator) (*pnet.PacketIO, error), frontendTLSConfig, backendTLSConfig *tls.Config) error {
	clientIO.ResetSequence()

	proxyCapability := auth.supportedServerCapabilities
	if frontendTLSConfig == nil {
		proxyCapability ^= pnet.ClientSSL
	}

	if err := clientIO.WriteInitialHandshake(proxyCapability.Uint32(), make([]byte, 20), mysql.AuthNativePassword); err != nil {
		return err
	}
	pkt, isSSL, err := clientIO.ReadSSLRequestOrHandshakeResp()
	if err != nil {
		return err
	}
	frontendCapability := pnet.Capability(binary.LittleEndian.Uint32(pkt))
	if isSSL {
		if _, err = clientIO.ServerTLSHandshake(frontendTLSConfig); err != nil {
			return err
		}
		pkt, _, err = clientIO.ReadSSLRequestOrHandshakeResp()
		if err != nil {
			return err
		}
		if len(pkt) <= 32 {
			return errors.WithStack(errors.New("expect handshake resp"))
		}
		frontendCapabilityResponse := pnet.Capability(binary.LittleEndian.Uint32(pkt))
		if frontendCapability != frontendCapabilityResponse {
			common := frontendCapability & frontendCapabilityResponse
			logger.Warn("frontend capabilities differs between SSL request and handshake response", zap.Stringer("common", common), zap.Stringer("ssl", frontendCapability^common), zap.Stringer("resp", frontendCapabilityResponse^common))
		}
	} else {
		binary.LittleEndian.PutUint32(pkt, (frontendCapability | pnet.ClientSSL).Uint32())
	}
	if commonCaps := frontendCapability & requiredFrontendCaps; commonCaps != requiredFrontendCaps {
		logger.Error("require frontend capabilities", zap.Stringer("common", commonCaps), zap.Stringer("required", requiredFrontendCaps))
		return errors.Wrapf(ErrCapabilityNegotiation, "require %s from frontend", requiredFrontendCaps&^commonCaps)
	}
	commonCaps := frontendCapability & proxyCapability
	if frontendCapability^commonCaps != 0 {
		logger.Debug("frontend send capabilities unsupported by proxy", zap.Stringer("common", commonCaps), zap.Stringer("frontend", frontendCapability^commonCaps), zap.Stringer("proxy", proxyCapability^commonCaps))
	}
	resp := pnet.ParseHandshakeResponse(pkt)
	auth.capability = commonCaps.Uint32()
	auth.user = resp.User
	auth.dbname = resp.DB
	auth.collation = resp.Collation
	auth.attrs = resp.Attrs

	backendIO, err := getBackendIO(auth)
	if err != nil {
		return err
	}

	backendIO.ResetSequence()
	// write proxy header
	if err := auth.writeProxyProtocol(clientIO, backendIO); err != nil {
		return err
	}

	// read backend initial handshake
	_, backendCapabilityU, err := auth.readInitialHandshake(backendIO)
	if err != nil {
		return err
	}
	backendCapability := pnet.Capability(backendCapabilityU)
	requiredBackendCaps := defRequiredBackendCaps
	if auth.requireBackendTLS {
		requiredBackendCaps |= pnet.ClientSSL
	}

	if commonCaps := backendCapability & requiredBackendCaps; commonCaps != requiredBackendCaps {
		// The error cannot be sent to the client because the client only expects an initial handshake packet.
		// The only way is to log it and disconnect.
		logger.Error("require backend capabilities", zap.Stringer("common", commonCaps), zap.Stringer("required", requiredBackendCaps^commonCaps))
		return errors.Wrapf(ErrCapabilityNegotiation, "require %s from backend", requiredBackendCaps^commonCaps)
	}
	if common := proxyCapability & backendCapability; (proxyCapability^common)&^pnet.ClientSSL != 0 {
		// TODO: need to do negotiation with backend
		// 1. proxyCapability &= backendCapability
		// 2. binary.LittleEndian.PutUint32(backendHandshake, proxyCapability.Uint32())
		//
		// it should exchange caps with the backend
		// but TiDB does not send all of its supported capabilities
		// thus we must ignore server capabilities
		// however, I will log something
		logger.Info("backend does not support capabilities from proxy", zap.Stringer("common", common), zap.Stringer("proxy", proxyCapability^common), zap.Stringer("backend", backendCapability^common))
	}

	// Send an unknown auth plugin so that the backend will request the auth data again.
	resp.AuthPlugin = "auth_unknown_plugin"
	resp.Capability = auth.capability

	if backendCapability&pnet.ClientSSL != 0 {
		resp.Capability |= mysql.ClientSSL
		pkt = pnet.MakeHandshakeResponse(resp)
		// write SSL Packet
		if err := backendIO.WritePacket(pkt[:32], true); err != nil {
			return err
		}
		auth.backendTLSConfig = backendTLSConfig.Clone()
		addr := backendIO.RemoteAddr().String()
		if auth.serverAddr != "" {
			// NOTE: should use DNS name as much as possible
			// Usually certs are signed with domain instead of IP addrs
			// And `RemoteAddr()` will return IP addr
			addr = auth.serverAddr
		}
		host, _, err := net.SplitHostPort(addr)
		if err == nil {
			auth.backendTLSConfig.ServerName = host
		}
		if err = backendIO.ClientTLSHandshake(auth.backendTLSConfig); err != nil {
			return err
		}
	} else {
		pkt = pnet.MakeHandshakeResponse(resp)
	}

	// forward client handshake resp
	if err := backendIO.WritePacket(pkt, true); err != nil {
		return err
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

func (auth *Authenticator) handshakeSecondTime(clientIO, backendIO *pnet.PacketIO, sessionToken string) error {
	if len(sessionToken) == 0 {
		return errors.New("session token is empty")
	}

	// write proxy header
	if err := auth.writeProxyProtocol(clientIO, backendIO); err != nil {
		return err
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
	// Always handshake with SSL enabled and enable auth_plugin.
	resp := &pnet.HandshakeResp{
		User:       auth.user,
		DB:         auth.dbname,
		AuthPlugin: mysql.AuthTiDBSessionToken,
		Attrs:      auth.attrs,
		AuthData:   authData,
		Capability: auth.capability | mysql.ClientSSL | mysql.ClientPluginAuth,
		Collation:  auth.collation,
	}
	data := pnet.MakeHandshakeResponse(resp)

	// write SSL req
	if err := backendIO.WritePacket(data[:32], true); err != nil {
		return err
	}
	// Send TLS / SSL request packet. The server must have supported TLS.
	if err := backendIO.ClientTLSHandshake(auth.backendTLSConfig); err != nil {
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
