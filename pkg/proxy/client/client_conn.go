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

package client

import (
	"context"
	"crypto/tls"
	"io"
	"net"

	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/pkg/manager/namespace"
	"github.com/pingcap/TiProxy/pkg/proxy/backend"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
	"go.uber.org/zap"
)

var (
	ErrClientConn = errors.New("this is an error from client")
)

type ClientConnection struct {
	logger            *zap.Logger
	frontendTLSConfig *tls.Config    // the TLS config to connect to clients.
	backendTLSConfig  *tls.Config    // the TLS config to connect to TiDB server.
	pkt               *pnet.PacketIO // a helper to read and write data in packet format.
	connMgr           *backend.BackendConnManager
}

func NewClientConnection(logger *zap.Logger, conn net.Conn, frontendTLSConfig *tls.Config, backendTLSConfig *tls.Config, nsmgr *namespace.NamespaceManager, connID uint64, proxyProtocol, requireBackendTLS bool) *ClientConnection {
	bemgr := backend.NewBackendConnManager(logger.Named("be"), nsmgr, connID, proxyProtocol, requireBackendTLS)
	opts := make([]pnet.PacketIOption, 0, 2)
	opts = append(opts, pnet.WithWrapError(ErrClientConn))
	if proxyProtocol {
		opts = append(opts, pnet.WithProxy)
	}
	pkt := pnet.NewPacketIO(conn, opts...)
	return &ClientConnection{
		logger:            logger.With(zap.Bool("proxy-protocol", proxyProtocol)),
		frontendTLSConfig: frontendTLSConfig,
		backendTLSConfig:  backendTLSConfig,
		pkt:               pkt,
		connMgr:           bemgr,
	}
}

func (cc *ClientConnection) Run(ctx context.Context) {
	var err error
	var msg string

	if err = cc.connMgr.Connect(ctx, cc.pkt, nil, cc.frontendTLSConfig, cc.backendTLSConfig); err != nil {
		msg = "new connection failed"
		goto clean
	}
	if err = cc.processMsg(ctx); err != nil {
		msg = "fails to relay the connection"
		goto clean
	}

clean:
	clientErr := errors.Is(err, ErrClientConn)
	if !(clientErr && errors.Is(err, io.EOF)) {
		cc.logger.Info(msg, zap.Error(err), zap.Bool("clientErr", clientErr), zap.Bool("serverErr", !clientErr))
	}
}

func (cc *ClientConnection) processMsg(ctx context.Context) error {
	for {
		cc.pkt.ResetSequence()
		clientPkt, err := cc.pkt.ReadPacket()
		if err != nil {
			return err
		}
		err = cc.connMgr.ExecuteCmd(ctx, clientPkt, cc.pkt)
		if err != nil {
			return err
		}
		cmd := clientPkt[0]
		switch cmd {
		case mysql.ComQuit:
			return nil
		}
	}
}

func (cc *ClientConnection) Close() error {
	return errors.Collect(ErrCloseConn, cc.pkt.Close(), cc.connMgr.Close())
}
