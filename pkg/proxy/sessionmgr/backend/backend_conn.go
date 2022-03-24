package backend

import (
	"errors"
	"net"
	"time"

	"github.com/pingcap/tidb/util/arena"
	pnet "github.com/tidb-incubator/weir/pkg/proxy/net"
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
	Connect() error
	PacketIO() *pnet.PacketIO
	Close() error
}

type BackendConnectionImpl struct {
	pkt         *pnet.PacketIO         // a helper to read and write data in packet format.
	bufReadConn *pnet.BufferedReadConn // a buffered-read net.Conn or buffered-read tls.Conn.
	alloc       arena.Allocator
	phase       connectionPhase
	capability  uint32
	address     string
}

func NewBackendConnectionImpl(address string) *BackendConnectionImpl {
	return &BackendConnectionImpl{
		phase:   handshaking,
		address: address,
		alloc:   arena.NewAllocator(32 * 1024),
	}
}

func (bc *BackendConnectionImpl) Connect() error {
	cn, err := net.DialTimeout("tcp", bc.address, time.Second*5)
	if err != nil {
		return errors.New("dial backend error")
	}

	bufReadConn := pnet.NewBufferedReadConn(cn)
	pkt := pnet.NewPacketIO(bufReadConn)
	bc.bufReadConn = bufReadConn
	bc.pkt = pkt
	return nil
}

func (bc *BackendConnectionImpl) PacketIO() *pnet.PacketIO {
	return bc.pkt
}

func (bc *BackendConnectionImpl) Close() error {
	return nil
}
