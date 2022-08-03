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
	"net"

	"github.com/pingcap/TiProxy/pkg/manager/namespace"
	"github.com/pingcap/TiProxy/pkg/proxy/backend"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type ClientConnection struct {
	serverTLSConfig  *tls.Config    // the TLS config to connect to clients.
	backendTLSConfig *tls.Config    // the TLS config to connect to TiDB server.
	pkt              *pnet.PacketIO // a helper to read and write data in packet format.
	connectionID     uint64
	nsmgr            *namespace.NamespaceManager
	ns               *namespace.Namespace
	connMgr          *backend.BackendConnManager
}

func NewClientConnection(conn net.Conn, connectionID uint64, serverTLSConfig *tls.Config, backendTLSConfig *tls.Config, nsmgr *namespace.NamespaceManager, bemgr *backend.BackendConnManager) *ClientConnection {
	pkt := pnet.NewPacketIO(conn)
	return &ClientConnection{
		serverTLSConfig:  serverTLSConfig,
		backendTLSConfig: backendTLSConfig,
		pkt:              pkt,
		connectionID:     connectionID,
		nsmgr:            nsmgr,
		connMgr:          bemgr,
	}
}

func (cc *ClientConnection) ConnectionID() uint64 {
	return cc.connectionID
}

func (cc *ClientConnection) Addr() string {
	return cc.pkt.RemoteAddr().String()
}

func (cc *ClientConnection) ConnectBackend(ctx context.Context) error {
	ns, ok := cc.nsmgr.GetNamespace("")
	if !ok {
		return errors.New("failed to find a namespace")
	}
	cc.ns = ns
	router := ns.GetRouter()
	addr, err := router.Route(cc.connMgr)
	if err != nil {
		return err
	}
	if err = cc.connMgr.Connect(ctx, addr, cc.pkt, cc.serverTLSConfig, cc.backendTLSConfig); err != nil {
		return err
	}
	return nil
}

func (cc *ClientConnection) Run(ctx context.Context) {
	if err := cc.ConnectBackend(ctx); err != nil {
		logutil.Logger(ctx).Info("new connection fails", zap.String("remoteAddr", cc.Addr()), zap.Error(err))
		metrics.HandShakeErrorCounter.Inc()
		err = cc.Close()
		terror.Log(errors.Trace(err))
		return
	}

	if err := cc.processMsg(ctx); err != nil {
		logutil.Logger(ctx).Info("process message fails", zap.Uint64("connID", cc.connectionID), zap.String("remoteAddr", cc.Addr()), zap.Error(err))
	} else {
		logutil.Logger(ctx).Debug("client connection disconnected normally", zap.Uint64("connID", cc.connectionID), zap.String("remoteAddr", cc.Addr()))
	}
}

func (cc *ClientConnection) processMsg(ctx context.Context) error {
	defer func() {
		err := cc.Close()
		terror.Log(errors.Trace(err))
	}()
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
	if err := cc.pkt.Close(); err != nil {
		terror.Log(err)
	}
	if err := cc.connMgr.Close(); err != nil {
		terror.Log(err)
	}
	return nil
}
