package backend

import (
	"context"

	"github.com/pingcap/parser/mysql"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/tidb-incubator/weir/pkg/proxy/driver"
	pnet "github.com/tidb-incubator/weir/pkg/proxy/net"
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

func (mgr *BackendConnManager) Connect(ctx context.Context, serverAddr string, clientIO *pnet.PacketIO) error {
	// It may be still connecting to the original backend server.
	if mgr.backendConn != nil {
		if err := mgr.backendConn.Close(); err != nil {
			return err
		}
	}
	mgr.backendConn = NewBackendConnectionImpl(serverAddr)
	err := mgr.backendConn.Connect()
	if err != nil {
		return err
	}
	backendIO := mgr.backendConn.PacketIO()
	return mgr.handshake(ctx, clientIO, backendIO)
}

func (mgr *BackendConnManager) handshake(ctx context.Context, clientIO, backendIO *pnet.PacketIO) error {
	backendIO.ResetSequence()
	for {
		serverPkt, err := backendIO.ReadPacket()
		if err != nil {
			return err
		}
		err = clientIO.WritePacket(serverPkt)
		if err != nil {
			return err
		}
		err = clientIO.Flush()
		if err != nil {
			return err
		}
		// Authentication finished.
		if len(serverPkt) > 1 {
			finished := false
			switch serverPkt[0] {
			case mysql.OKHeader, mysql.ErrHeader:
				finished = true
			case mysql.EOFHeader:
				// EOFHeader == AuthSwitchRequest. Not possible to send EOF now.
			}
			if finished {
				break
			}
		}
		clientPkt, err := clientIO.ReadPacket()
		if err != nil {
			return err
		}
		err = backendIO.WritePacket(clientPkt)
		if err != nil {
			return err
		}
		err = backendIO.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func (mgr *BackendConnManager) ExecuteCmd(ctx context.Context, request []byte, clientIO *pnet.PacketIO) error {
	backendIO := mgr.backendConn.PacketIO()
	backendIO.ResetSequence()
	err := backendIO.WritePacket(request)
	if err != nil {
		return nil
	}
	err = backendIO.Flush()
	if err != nil {
		return err
	}
	cmd := request[0]
	switch cmd {
	case mysql.ComQuery:
		return mgr.forwardCommand(ctx, clientIO, backendIO, 2)
	case mysql.ComQuit:
		return nil
	default:
		return mgr.forwardCommand(ctx, clientIO, backendIO, 1)
	}
}

func (mgr *BackendConnManager) forwardCommand(ctx context.Context, clientIO, backendIO *pnet.PacketIO, expectedEOFNum int) error {
	eofHeaders := 0
	okOrErr := false
	for eofHeaders < expectedEOFNum && !okOrErr {
		data, err := backendIO.ReadPacket()
		if err != nil {
			return err
		}
		switch data[0] {
		case mysql.OKHeader:
			mgr.handleOKPacket(data)
			okOrErr = true
		case mysql.ErrHeader:
			okOrErr = true
		case mysql.EOFHeader:
			if len(data) <= 5 {
				mgr.handleEOFPacket(data)
				eofHeaders += 1
			}
		}
		err = clientIO.WritePacket(data)
		if err != nil {
			return err
		}
	}
	return clientIO.Flush()
}

func (mgr *BackendConnManager) handleOKPacket(data []byte) {
}

func (mgr *BackendConnManager) handleEOFPacket(data []byte) {
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
