package driver

import (
	"crypto/tls"
	"net"
)

type createClientConnFunc func(QueryCtx, net.Conn, uint64, *tls.Config, uint32) ClientConnection

type DriverImpl struct {
	nsmgr                NamespaceManager
	createClientConnFunc createClientConnFunc
}

func NewDriverImpl(nsmgr NamespaceManager, createClientConnFunc createClientConnFunc) *DriverImpl {
	return &DriverImpl{
		nsmgr:                nsmgr,
		createClientConnFunc: createClientConnFunc,
	}
}

func (d *DriverImpl) CreateClientConnection(conn net.Conn, connectionID uint64, tlsConfig *tls.Config, serverCapability uint32) ClientConnection {
	queryCtx := NewQueryCtxImpl(d.nsmgr, connectionID)
	return d.createClientConnFunc(queryCtx, conn, connectionID, tlsConfig, serverCapability)
}
