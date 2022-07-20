// Copyright 2020 Ipalfish, Inc.
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

	"github.com/pingcap/TiProxy/pkg/proxy/driver"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type ClientConnectionImpl struct {
	serverTLSConfig  *tls.Config            // the TLS config to connect to clients.
	backendTLSConfig *tls.Config            // the TLS config to connect to TiDB server.
	pkt              *pnet.PacketIO         // a helper to read and write data in packet format.
	bufReadConn      *pnet.BufferedReadConn // a buffered-read net.Conn or buffered-read tls.Conn.
	queryCtx         driver.QueryCtx
	connectionID     uint64
}

func NewClientConnectionImpl(queryCtx driver.QueryCtx, conn net.Conn, connectionID uint64, serverTLSConfig, backendTLSConfig *tls.Config) driver.ClientConnection {
	bufReadConn := pnet.NewBufferedReadConn(conn)
	pkt := pnet.NewPacketIO(bufReadConn)
	return &ClientConnectionImpl{
		queryCtx:         queryCtx,
		serverTLSConfig:  serverTLSConfig,
		backendTLSConfig: backendTLSConfig,
		bufReadConn:      bufReadConn,
		pkt:              pkt,
		connectionID:     connectionID,
	}
}

func (cc *ClientConnectionImpl) ConnectionID() uint64 {
	return cc.connectionID
}

func (cc *ClientConnectionImpl) Addr() string {
	return cc.bufReadConn.RemoteAddr().String()
}

func (cc *ClientConnectionImpl) Run(ctx context.Context) {
	if err := cc.queryCtx.ConnectBackend(ctx, cc.pkt, cc.serverTLSConfig, cc.backendTLSConfig); err != nil {
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

func (cc *ClientConnectionImpl) processMsg(ctx context.Context) error {
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
		err = cc.queryCtx.ExecuteCmd(ctx, clientPkt, cc.pkt)
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

func (cc *ClientConnectionImpl) Close() error {
	if err := cc.pkt.Close(); err != nil {
		terror.Log(err)
	}
	return cc.queryCtx.Close()
}
