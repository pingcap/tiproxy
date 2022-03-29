package driver

import (
	"crypto/tls"
	"net"
)

type createClientConnFunc func(QueryCtx, net.Conn, uint64, *tls.Config) ClientConnection
type createBackendConnMgrFunc func() BackendConnManager

type DriverImpl struct {
	nsmgr                    NamespaceManager
	createClientConnFunc     createClientConnFunc
	createBackendConnMgrFunc createBackendConnMgrFunc
}

func NewDriverImpl(nsmgr NamespaceManager, createClientConnFunc createClientConnFunc, createBackendConnMgrFunc createBackendConnMgrFunc) *DriverImpl {
	return &DriverImpl{
		nsmgr:                    nsmgr,
		createClientConnFunc:     createClientConnFunc,
		createBackendConnMgrFunc: createBackendConnMgrFunc,
	}
}

func (d *DriverImpl) CreateClientConnection(conn net.Conn, connectionID uint64, tlsConfig *tls.Config) ClientConnection {
	backendConnMgr := d.createBackendConnMgrFunc()
	queryCtx := NewQueryCtxImpl(d.nsmgr, backendConnMgr, connectionID)
	return d.createClientConnFunc(queryCtx, conn, connectionID, tlsConfig)
}
