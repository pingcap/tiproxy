package driver

import (
	"context"
	"crypto/tls"
	"errors"

	pnet "github.com/djshow832/weir/pkg/proxy/net"
	"github.com/pingcap/tidb/parser"
)

// Server information.
const (
	ServerStatusInTrans            uint16 = 0x0001
	ServerStatusAutocommit         uint16 = 0x0002
	ServerMoreResultsExists        uint16 = 0x0008
	ServerStatusNoGoodIndexUsed    uint16 = 0x0010
	ServerStatusNoIndexUsed        uint16 = 0x0020
	ServerStatusCursorExists       uint16 = 0x0040
	ServerStatusLastRowSend        uint16 = 0x0080
	ServerStatusDBDropped          uint16 = 0x0100
	ServerStatusNoBackslashEscaped uint16 = 0x0200
	ServerStatusMetadataChanged    uint16 = 0x0400
	ServerStatusWasSlow            uint16 = 0x0800
	ServerPSOutParams              uint16 = 0x1000
)

type QueryCtxImpl struct {
	connId  uint64
	nsmgr   NamespaceManager
	ns      Namespace
	parser  *parser.Parser
	connMgr BackendConnManager
}

func NewQueryCtxImpl(nsmgr NamespaceManager, backendConnMgr BackendConnManager, connId uint64) *QueryCtxImpl {
	return &QueryCtxImpl{
		connId:  connId,
		nsmgr:   nsmgr,
		parser:  parser.New(),
		connMgr: backendConnMgr,
	}
}

func (q *QueryCtxImpl) ExecuteCmd(ctx context.Context, request []byte, clientIO *pnet.PacketIO) error {
	return q.connMgr.ExecuteCmd(ctx, request, clientIO)
}

func (q *QueryCtxImpl) Close() error {
	if q.connMgr != nil {
		return q.connMgr.Close()
	}
	if q.ns != nil {
		q.ns.DescConnCount()
	}
	return nil
}

func (q *QueryCtxImpl) ConnectBackend(ctx context.Context, clientIO *pnet.PacketIO, serverTLSConfig, backendTLSConfig *tls.Config) error {
	ns, ok := q.nsmgr.Auth("", nil, nil)
	if !ok {
		return errors.New("failed to find a namespace")
	}
	q.ns = ns
	router := ns.GetRouter()
	addr, err := router.Route()
	if err != nil {
		return err
	}
	q.connMgr.SetEventReceiver(router)
	if err = q.connMgr.Connect(ctx, addr, clientIO, serverTLSConfig, backendTLSConfig); err != nil {
		return err
	}
	q.ns.IncrConnCount()
	return nil
}
