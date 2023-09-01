// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/proxy/proxyprotocol"
	"go.uber.org/zap"
)

var (
	ErrCapabilityNegotiation = errors.New("capability negotiation failed")
	ErrTLSConfigRequired     = errors.New("require TLS config on TiProxy when require-backend-tls=true")
)

const unknownAuthPlugin = "auth_unknown_plugin"
const requiredFrontendCaps = pnet.ClientProtocol41
const defRequiredBackendCaps = pnet.ClientDeprecateEOF

// SupportedServerCapabilities is the default supported capabilities. Other server capabilities are not supported.
// TiDB supports ClientDeprecateEOF since v6.3.0.
const SupportedServerCapabilities = pnet.ClientLongPassword | pnet.ClientFoundRows | pnet.ClientConnectWithDB |
	pnet.ClientODBC | pnet.ClientLocalFiles | pnet.ClientInteractive | pnet.ClientLongFlag | pnet.ClientSSL |
	pnet.ClientTransactions | pnet.ClientReserved | pnet.ClientSecureConnection | pnet.ClientMultiStatements |
	pnet.ClientMultiResults | pnet.ClientPluginAuth | pnet.ClientConnectAttrs | pnet.ClientPluginAuthLenencClientData |
	requiredFrontendCaps | defRequiredBackendCaps

// Authenticator handshakes with the client and the backend.
type Authenticator struct {
	dbname            string // default database name
	user              string
	attrs             map[string]string
	salt              []byte
	capability        pnet.Capability
	collation         uint8
	proxyProtocol     bool
	requireBackendTLS bool
}

func (auth *Authenticator) String() string {
	return fmt.Sprintf("user:%s, dbname:%s, capability:%d, collation:%d",
		auth.user, auth.dbname, auth.capability, auth.collation)
}

func (auth *Authenticator) writeProxyProtocol(clientIO, backendIO *pnet.PacketIO) error {
	if auth.proxyProtocol {
		proxy := clientIO.Proxy()
		if proxy == nil {
			proxy = &proxyprotocol.Proxy{
				SrcAddress: clientIO.RemoteAddr(),
				DstAddress: backendIO.RemoteAddr(),
				Version:    proxyprotocol.ProxyVersion2,
			}
		}
		// either from another proxy or directly from clients, we are acting as a proxy
		proxy.Command = proxyprotocol.ProxyCommandProxy
		if err := backendIO.WriteProxyV2(proxy); err != nil {
			return err
		}
	}
	return nil
}

func (auth *Authenticator) verifyBackendCaps(logger *zap.Logger, backendCapability pnet.Capability) error {
	requiredBackendCaps := defRequiredBackendCaps & pnet.Capability(auth.capability)
	if auth.requireBackendTLS {
		requiredBackendCaps |= pnet.ClientSSL
	}

	if commonCaps := backendCapability & requiredBackendCaps; commonCaps != requiredBackendCaps {
		// The error cannot be sent to the client because the client only expects an initial handshake packet.
		// The only way is to log it and disconnect.
		logger.Error("require backend capabilities", zap.Stringer("common", commonCaps), zap.Stringer("required", requiredBackendCaps^commonCaps))
		return errors.Wrapf(ErrCapabilityNegotiation, "require %s from backend", requiredBackendCaps^commonCaps)
	}

	return nil
}

type backendIOGetter func(ctx ConnContext, auth *Authenticator, resp *pnet.HandshakeResp, timeout time.Duration) (*pnet.PacketIO, error)

func (auth *Authenticator) handshakeFirstTime(logger *zap.Logger, cctx ConnContext, clientIO *pnet.PacketIO, handshakeHandler HandshakeHandler,
	getBackendIO backendIOGetter, frontendTLSConfig, backendTLSConfig *tls.Config) error {
	clientIO.ResetSequence()

	proxyCapability := handshakeHandler.GetCapability()
	if frontendTLSConfig == nil {
		proxyCapability ^= pnet.ClientSSL
	}

	cid, _ := cctx.Value(ConnContextKeyConnID).(uint64)
	if err := clientIO.WriteInitialHandshake(proxyCapability, auth.salt, mysql.AuthNativePassword, handshakeHandler.GetServerVersion(), cid); err != nil {
		return err
	}
	pkt, isSSL, err := clientIO.ReadSSLRequestOrHandshakeResp()
	if err != nil {
		return err
	}
	frontendCapability := pnet.Capability(binary.LittleEndian.Uint32(pkt))
	if isSSL {
		if _, err = clientIO.ServerTLSHandshake(frontendTLSConfig); err != nil {
			return pnet.WrapUserError(err, err.Error())
		}
		pkt, _, err = clientIO.ReadSSLRequestOrHandshakeResp()
		if err != nil {
			return err
		}
		frontendCapabilityResponse := pnet.Capability(binary.LittleEndian.Uint32(pkt))
		if frontendCapability != frontendCapabilityResponse {
			common := frontendCapability & frontendCapabilityResponse
			logger.Warn("frontend capabilities differs between SSL request and handshake response", zap.Stringer("common", common), zap.Stringer("ssl", frontendCapability^common), zap.Stringer("resp", frontendCapabilityResponse^common))
			// Some drivers don't set ClientSSL in the second packet, but the HandshakeHandler wants the real capability.
			binary.LittleEndian.PutUint32(pkt, frontendCapability.Uint32())
		}
	}
	if commonCaps := frontendCapability & requiredFrontendCaps; commonCaps != requiredFrontendCaps {
		logger.Error("require frontend capabilities", zap.Stringer("common", commonCaps), zap.Stringer("required", requiredFrontendCaps))
		if writeErr := clientIO.WriteErrPacket(mysql.ErrNotSupportedAuthMode); writeErr != nil {
			return writeErr
		}
		return errors.Wrapf(ErrCapabilityNegotiation, "require %s from frontend", requiredFrontendCaps&^commonCaps)
	}
	commonCaps := frontendCapability & proxyCapability
	if frontendCapability^commonCaps != 0 {
		logger.Debug("frontend send capabilities unsupported by proxy", zap.Stringer("common", commonCaps), zap.Stringer("frontend", frontendCapability^commonCaps), zap.Stringer("proxy", proxyCapability^commonCaps))
	}
	auth.capability = commonCaps
	if auth.capability&pnet.ClientPluginAuth == 0 {
		logger.Warn("frontend may not support plugin auth", zap.Stringer("capability", commonCaps))
		// Some clients (e.g. node/mysql) support ClientAuthPlugin but don't have the capability set correctly.
		// Always set it to ensure capability.
		auth.capability |= pnet.ClientPluginAuth
	}

	if isSSL {
		cctx.SetValue(ConnContextKeyTLSState, clientIO.TLSConnectionState())
	}
	clientResp, err := pnet.ParseHandshakeResponse(pkt)
	var warning *errors.Warning
	if errors.As(err, &warning) {
		logger.Warn("parse handshake response encounters error", zap.Error(err))
	} else if err != nil {
		return pnet.WrapUserError(err, parsePktErrMsg)
	}
	if err = handshakeHandler.HandleHandshakeResp(cctx, clientResp); err != nil {
		return pnet.WrapUserError(err, err.Error())
	}
	auth.user = clientResp.User
	auth.dbname = clientResp.DB
	auth.collation = clientResp.Collation
	auth.attrs = clientResp.Attrs

	// In case of testing, backendIO is passed manually that we don't want to bother with the routing logic.
	backendIO, err := getBackendIO(cctx, auth, clientResp, 15*time.Second)
	if err != nil {
		return pnet.WrapUserError(err, connectErrMsg)
	}
	backendIO.ResetSequence()

	// write proxy header
	if err := auth.writeProxyProtocol(clientIO, backendIO); err != nil {
		return pnet.WrapUserError(err, handshakeErrMsg)
	}

	// read backend initial handshake
	serverPkt, backendCapability, err := auth.readInitialHandshake(backendIO)
	if err != nil {
		if IsMySQLError(err) {
			if writeErr := clientIO.WritePacket(serverPkt, true); writeErr != nil {
				err = writeErr
			}
			return err
		}
		return pnet.WrapUserError(err, handshakeErrMsg)
	}

	if err := auth.verifyBackendCaps(logger, backendCapability); err != nil {
		return pnet.WrapUserError(err, capabilityErrMsg)
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
		logger.Debug("backend does not support capabilities from proxy", zap.Stringer("common", common), zap.Stringer("proxy", proxyCapability^common), zap.Stringer("backend", backendCapability^common))
	}

	// forward client handshake resp
	if err := auth.writeAuthHandshake(
		backendIO, backendTLSConfig, backendCapability,
		// Send an unknown auth plugin so that the backend will request the auth data again.
		// Copy the auth data so that the backend can set correct `using password` in the error message.
		unknownAuthPlugin, clientResp.AuthData, 0,
	); err != nil {
		return pnet.WrapUserError(err, handshakeErrMsg)
	}

	// forward other packets
	pluginName := ""
loop:
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
			if serverPkt[0] == mysql.AuthSwitchRequest {
				pluginName = string(serverPkt[1 : bytes.IndexByte(serverPkt[1:], 0)+1])
			} else if serverPkt[0] == 1 && pluginName == mysql.AuthCachingSha2Password && len(serverPkt) == 2 && serverPkt[1] == 3 {
				// caching_sha2_password fast path
				continue loop
			}
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

func (auth *Authenticator) handshakeSecondTime(logger *zap.Logger, clientIO, backendIO *pnet.PacketIO, backendTLSConfig *tls.Config, sessionToken string) error {
	if len(sessionToken) == 0 {
		return errors.New("session token is empty")
	}

	// write proxy header
	if err := auth.writeProxyProtocol(clientIO, backendIO); err != nil {
		return err
	}

	_, backendCapability, err := auth.readInitialHandshake(backendIO)
	if err != nil {
		return err
	}

	if err := auth.verifyBackendCaps(logger, pnet.Capability(backendCapability)); err != nil {
		return err
	}

	if err = auth.writeAuthHandshake(
		backendIO, backendTLSConfig, backendCapability,
		pnet.AuthTiDBSessionToken, hack.Slice(sessionToken), pnet.ClientPluginAuth,
	); err != nil {
		return err
	}

	return auth.handleSecondAuthResult(backendIO)
}

func (auth *Authenticator) readInitialHandshake(backendIO *pnet.PacketIO) (serverPkt []byte, capability pnet.Capability, err error) {
	if serverPkt, err = backendIO.ReadPacket(); err != nil {
		return
	}
	if pnet.IsErrorPacket(serverPkt) {
		err = pnet.ParseErrorPacket(serverPkt)
		return
	}
	capability, _, _ = pnet.ParseInitialHandshake(serverPkt)
	return
}

func (auth *Authenticator) writeAuthHandshake(
	backendIO *pnet.PacketIO,
	backendTLSConfig *tls.Config,
	backendCapability pnet.Capability,
	authPlugin string,
	authData []byte,
	authCap pnet.Capability,
) error {
	// Always handshake with SSL enabled and enable auth_plugin.
	resp := &pnet.HandshakeResp{
		User:       auth.user,
		DB:         auth.dbname,
		Attrs:      auth.attrs,
		Collation:  auth.collation,
		AuthData:   authData,
		Capability: auth.capability | authCap,
		AuthPlugin: authPlugin,
	}

	if len(resp.Attrs) > 0 {
		resp.Capability |= pnet.ClientConnectAttrs
	}

	var pkt []byte
	var enableTLS bool
	if auth.requireBackendTLS {
		if backendTLSConfig == nil {
			return ErrTLSConfigRequired
		}
		enableTLS = true
	} else {
		// When client TLS is disabled, also disables proxy TLS.
		enableTLS = pnet.Capability(auth.capability)&pnet.ClientSSL != 0 && backendCapability&pnet.ClientSSL != 0 && backendTLSConfig != nil
	}
	if enableTLS {
		resp.Capability |= pnet.ClientSSL
		pkt = pnet.MakeHandshakeResponse(resp)
		// write SSL Packet
		if err := backendIO.WritePacket(pkt[:32], true); err != nil {
			return err
		}
		// Send TLS / SSL request packet. The server must have supported TLS.
		tcfg := backendTLSConfig.Clone()
		addr := backendIO.RemoteAddr().String()
		host, _, err := net.SplitHostPort(addr)
		if err == nil {
			tcfg.ServerName = host
		}
		if err := backendIO.ClientTLSHandshake(tcfg); err != nil {
			return err
		}
	} else {
		resp.Capability &= ^pnet.ClientSSL
		pkt = pnet.MakeHandshakeResponse(resp)
	}

	// write handshake resp
	return backendIO.WritePacket(pkt, true)
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
