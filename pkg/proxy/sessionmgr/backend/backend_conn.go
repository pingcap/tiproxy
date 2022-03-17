package backend

import (
	"github.com/siddontang/go-mysql/packet"
	"github.com/tidb-incubator/weir/pkg/util/auth"
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
	Connect(authInfo *auth.AuthInfo) error
	Close() error
}

type BackendConnectionImpl struct {
	*packet.Conn

	phase      connectionPhase
	capability uint32
	address    string
}

func NewBackendConnectionImpl(address string) *BackendConnectionImpl {
	return &BackendConnectionImpl{
		phase:   handshaking,
		address: address,
	}
}

func (conn *BackendConnectionImpl) Connect(authInfo *auth.AuthInfo) error {
	return nil
}

func (conn *BackendConnectionImpl) Close() error {
	return nil
}
