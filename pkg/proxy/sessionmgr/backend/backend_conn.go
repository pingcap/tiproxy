package backend

import (
	"github.com/siddontang/go-mysql/packet"
)

type connectionPhase byte

const (
	handshaking connectionPhase = iota
	authenticating
	command
	fail
	disconnecting
)

type BackendConnection interface {
	Connect(username string, authData []byte) error
	Close() error
}

type BackendConnectionImpl struct {
	*packet.Conn

	phase      connectionPhase
	capability uint32
	server     *BackendServer
}

func NewBackendConnectionImpl(backendServer *BackendServer) *BackendConnectionImpl {
	return &BackendConnectionImpl{
		phase:  handshaking,
		server: backendServer,
	}
}

func (conn *BackendConnectionImpl) Connect(username string, authData []byte) error {
	return nil
}

func (conn *BackendConnectionImpl) Close() error {
	return nil
}
