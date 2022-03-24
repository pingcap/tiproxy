package client

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tidb-incubator/weir/pkg/proxy/driver"
	pnet "github.com/tidb-incubator/weir/pkg/proxy/net"
	"go.uber.org/zap"
)

type ClientConnectionImpl struct {
	tlsConn          *tls.Conn // TLS connection, nil if not TLS.
	tlsConfig        *tls.Config
	pkt              *pnet.PacketIO         // a helper to read and write data in packet format.
	bufReadConn      *pnet.BufferedReadConn // a buffered-read net.Conn or buffered-read tls.Conn.
	alloc            arena.Allocator
	queryCtx         driver.QueryCtx
	connectionID     uint64
	user             string // user of the client.
	dbname           string // default database name.
	serverCapability uint32
	capability       uint32 // final capability
	collation        uint8
	attrs            map[string]string // attributes parsed from client handshake response, not used for now.
}

func NewClientConnectionImpl(queryCtx driver.QueryCtx, conn net.Conn, connectionID uint64, tlsConfig *tls.Config, serverCapability uint32) driver.ClientConnection {
	bufReadConn := pnet.NewBufferedReadConn(conn)
	pkt := pnet.NewPacketIO(bufReadConn)
	return &ClientConnectionImpl{
		queryCtx:         queryCtx,
		tlsConfig:        tlsConfig,
		serverCapability: serverCapability,
		alloc:            arena.NewAllocator(32 * 1024),
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

func (cc *ClientConnectionImpl) Auth() error {
	return nil
}

func (cc *ClientConnectionImpl) Run(ctx context.Context) {
	if err := cc.queryCtx.ConnectBackend(ctx, cc.pkt); err != nil {
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
			var dataStr string
			if len(clientPkt) > 1 && clientPkt[len(clientPkt)-1] == 0 {
				data = clientPkt[1 : len(clientPkt)-1]
			} else {
				data = clientPkt[1:]
			}
			dataStr = string(hack.String(data))
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

func (cc *ClientConnectionImpl) Close() error {
	return nil
}
