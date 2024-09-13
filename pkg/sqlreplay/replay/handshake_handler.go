// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
)

var _ backend.HandshakeHandler = (*handshakeHandler)(nil)

type handshakeHandler struct {
	backend.HandshakeHandler
}

func NewHandshakeHandler(hsHsHandler backend.HandshakeHandler) *handshakeHandler {
	return &handshakeHandler{
		HandshakeHandler: hsHsHandler,
	}
}

func (handler *handshakeHandler) GetCapability() pnet.Capability {
	return pnet.ClientLongPassword | pnet.ClientFoundRows |
		pnet.ClientODBC | pnet.ClientLocalFiles | pnet.ClientInteractive | pnet.ClientLongFlag | pnet.ClientSSL |
		pnet.ClientTransactions | pnet.ClientReserved | pnet.ClientSecureConnection |
		pnet.ClientMultiResults | pnet.ClientPluginAuth | pnet.ClientPluginAuthLenencClientData |
		pnet.ClientProtocol41 | pnet.ClientDeprecateEOF
}
