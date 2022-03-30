package client

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/djshow832/weir/pkg/proxy/driver"
	pnet "github.com/djshow832/weir/pkg/proxy/net"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type ClientConnectionImpl struct {
	tlsConfig    *tls.Config
	pkt          *pnet.PacketIO         // a helper to read and write data in packet format.
	bufReadConn  *pnet.BufferedReadConn // a buffered-read net.Conn or buffered-read tls.Conn.
	alloc        arena.Allocator
	queryCtx     driver.QueryCtx
	connectionID uint64
}

func NewClientConnectionImpl(queryCtx driver.QueryCtx, conn net.Conn, connectionID uint64, tlsConfig *tls.Config) driver.ClientConnection {
	bufReadConn := pnet.NewBufferedReadConn(conn)
	pkt := pnet.NewPacketIO(bufReadConn)
	return &ClientConnectionImpl{
		queryCtx:     queryCtx,
		tlsConfig:    tlsConfig,
		alloc:        arena.NewAllocator(32 * 1024),
		bufReadConn:  bufReadConn,
		pkt:          pkt,
		connectionID: connectionID,
	}
}

func (cc *ClientConnectionImpl) ConnectionID() uint64 {
	return cc.connectionID
}

func (cc *ClientConnectionImpl) Addr() string {
	return cc.bufReadConn.RemoteAddr().String()
}

func (cc *ClientConnectionImpl) Auth() error {
	return nil
}

func (cc *ClientConnectionImpl) Run(ctx context.Context) {
	if err := cc.queryCtx.ConnectBackend(ctx, cc.pkt, cc.tlsConfig); err != nil {
		logutil.Logger(ctx).Info("new connection fails", zap.String("remoteAddr", cc.Addr()), zap.Error(err))
		metrics.HandShakeErrorCounter.Inc()
		err = cc.Close()
		terror.Log(errors.Trace(err))
		return
	}
	logutil.Logger(ctx).Info("new connection succeeds", zap.String("remoteAddr", cc.Addr()))

	if err := cc.processMsg(ctx); err != nil {
		logutil.Logger(ctx).Info("forward message fails", zap.String("remoteAddr", cc.Addr()), zap.Error(err))
	}
}

func (cc *ClientConnectionImpl) processMsg(ctx context.Context) error {
	for {
		cc.pkt.ResetSequence()
		clientPkt, err := cc.pkt.ReadPacket()
		if err != nil {
			return err
		}
		cmd := clientPkt[0]
		cmdStr, ok := mysql.Command2Str[cmd]
		if !ok {
			cmdStr = "unknown"
		}
		switch cmd {
		case mysql.ComQuery:
			var data []byte
			if len(clientPkt) > 1 && clientPkt[len(clientPkt)-1] == 0 {
				data = clientPkt[1 : len(clientPkt)-1]
			} else {
				data = clientPkt[1:]
			}
			dataStr := string(hack.String(data))
			logutil.Logger(ctx).Info("receive cmd", zap.String("query", dataStr))
		case mysql.ComQuit:
			logutil.Logger(ctx).Info("quit")
			return nil
		default:
			logutil.Logger(ctx).Info("receive cmd", zap.String("cmd", cmdStr))
		}
		err = cc.queryCtx.ExecuteCmd(ctx, clientPkt, cc.pkt)
		if err != nil {
			return err
		}
	}
}

func (cc *ClientConnectionImpl) Redirect() error {
	return cc.queryCtx.Redirect(cc.tlsConfig)
}

func (cc *ClientConnectionImpl) Close() error {
	return cc.queryCtx.Close()
}
