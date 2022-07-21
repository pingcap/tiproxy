// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"errors"
	"net"
	"time"

	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/tidb/util/arena"
)

const (
	DialTimeout = 5 * time.Second
)

type connectionPhase byte

type BackendConnection interface {
	Addr() string
	Connect() error
	PacketIO() *pnet.PacketIO
	Close() error
}

type BackendConnectionImpl struct {
	pkt        *pnet.PacketIO // a helper to read and write data in packet format.
	alloc      arena.Allocator
	phase      connectionPhase
	capability uint32
	address    string
}

func NewBackendConnectionImpl(address string) *BackendConnectionImpl {
	return &BackendConnectionImpl{
		address: address,
		alloc:   arena.NewAllocator(32 * 1024),
	}
}

func (bc *BackendConnectionImpl) Addr() string {
	return bc.address
}

func (bc *BackendConnectionImpl) Connect() error {
	cn, err := net.DialTimeout("tcp", bc.address, DialTimeout)
	if err != nil {
		return errors.New("dial backend error")
	}

	bufReadConn := pnet.NewBufferedReadConn(cn)
	pkt := pnet.NewPacketIO(bufReadConn)
	bc.pkt = pkt
	return nil
}

func (bc *BackendConnectionImpl) PacketIO() *pnet.PacketIO {
	return bc.pkt
}

func (bc *BackendConnectionImpl) Close() error {
	if bc.pkt != nil {
		return bc.pkt.Close()
	}
	return nil
}
