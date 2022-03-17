package backend

import (
	"context"

	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/tidb-incubator/weir/pkg/proxy/driver"
	"github.com/tidb-incubator/weir/pkg/util/auth"
)

type ConnectionPhase byte

const (
	InitBackend ConnectionPhase = iota
	InitStates
	Command
	WaitFinish
	QueryStates
	Disconnect
)

const (
	StatusInTrans            uint32 = 0x01
	StatusAutoCommit         uint32 = 0x02
	StatusPrepareWaitExecute uint32 = 0x04
	StatusPrepareWaitFetch   uint32 = 0x08
)

type BackendConnManager struct {
	backendConn     BackendConnection
	connectionPhase ConnectionPhase
	serverStatus    uint32
	authInfo        *auth.AuthInfo
}

func NewBackendConnManager() driver.BackendConnManager {
	return &BackendConnManager{
		connectionPhase: InitBackend,
		serverStatus:    StatusAutoCommit,
	}
}

func (mgr *BackendConnManager) Run(context context.Context) {
	for {
		switch mgr.connectionPhase {
		case InitBackend:
		case InitStates:
		case Command, WaitFinish:
		case QueryStates:
		case Disconnect:
		}
	}
}

func (mgr *BackendConnManager) SetAuthInfo(authInfo *auth.AuthInfo) {
	mgr.authInfo = authInfo
}

func (mgr *BackendConnManager) Connect(address string) error {
	// It may be still connecting to the original backend server.
	if mgr.backendConn != nil {
		if err := mgr.backendConn.Close(); err != nil {
			return err
		}
	}
	mgr.backendConn = NewBackendConnectionImpl(address)
	return mgr.backendConn.Connect(mgr.authInfo)
}

func (mgr *BackendConnManager) initSessionStates() error {
	return nil
}

func (mgr *BackendConnManager) dispatchCmd() error {
	return nil
}

func (mgr *BackendConnManager) querySessionStates() error {
	return nil
}

func (mgr *BackendConnManager) disconnect() error {
	return nil
}

func (mgr *BackendConnManager) PrepareRedirect(newAddr string) error {
	return nil
}

func (mgr *BackendConnManager) redirect() error {
	return nil
}

func (mgr *BackendConnManager) Close() error {
	return nil
}

func (mgr *BackendConnManager) Query(ctx context.Context, sql string) (*gomysql.Result, error) {
	return nil, nil
}
