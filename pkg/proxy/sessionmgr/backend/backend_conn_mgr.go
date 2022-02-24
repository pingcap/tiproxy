package backend

import (
	"context"

	gomysql "github.com/siddontang/go-mysql/mysql"
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

type AuthInfo struct {
	// user information obtained during authentication
	Username string
	AuthData []byte
}

type BackendConnManager struct {
	backendConn     BackendConnection
	connectionPhase ConnectionPhase
	serverStatus    uint32
	authInfo        *AuthInfo
}

func NewBackendConnManager() *BackendConnManager {
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

func (mgr *BackendConnManager) SetAuthInfo(authInfo *AuthInfo) {
	mgr.authInfo = authInfo
}

func (mgr *BackendConnManager) Connect(server *BackendServer) error {
	if mgr.backendConn != nil {
		if err := mgr.backendConn.Close(); err != nil {
			return err
		}
	}
	mgr.backendConn = NewBackendConnectionImpl(server)
	return mgr.backendConn.Connect(mgr.authInfo.Username, mgr.authInfo.AuthData)
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
