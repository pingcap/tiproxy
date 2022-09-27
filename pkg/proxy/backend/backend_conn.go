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
	"net"
	"time"

	"github.com/pingcap/TiProxy/lib/util/errors"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
)

const (
	DialTimeout = 5 * time.Second
)

type BackendConnection struct {
	pkt     *pnet.PacketIO // a helper to read and write data in packet format.
	address string
}

func NewBackendConnection(address string) *BackendConnection {
	return &BackendConnection{
		address: address,
	}
}

func (bc *BackendConnection) Addr() string {
	return bc.address
}

func (bc *BackendConnection) Connect() error {
	cn, err := net.DialTimeout("tcp", bc.address, DialTimeout)
	if err != nil {
		return errors.Wrapf(err, "dial backend error")
	}

	pkt := pnet.NewPacketIO(cn)
	bc.pkt = pkt
	return nil
}

func (bc *BackendConnection) PacketIO() *pnet.PacketIO {
	return bc.pkt
}

func (bc *BackendConnection) Close() error {
	if bc.pkt != nil {
		return bc.pkt.Close()
	}
	return nil
}
