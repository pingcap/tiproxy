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
	"crypto/tls"

	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
)

type proxyConfig struct {
	frontendTLSConfig *tls.Config
	backendTLSConfig  *tls.Config
	sessionToken      string
}

type mockProxy struct {
	*proxyConfig
	auth         *Authenticator
	cmdProcessor *CmdProcessor
}

func newMockProxy(cfg *proxyConfig) *mockProxy {
	return &mockProxy{
		proxyConfig:  cfg,
		auth:         new(Authenticator),
		cmdProcessor: NewCmdProcessor(),
	}
}

func (mp *mockProxy) authenticateFirstTime(clientIO, backendIO *pnet.PacketIO) error {
	_, err := mp.auth.handshakeFirstTime(clientIO, backendIO, mp.frontendTLSConfig, mp.backendTLSConfig)
	return err
}

func (mp *mockProxy) authenticateSecondTime(_, backendIO *pnet.PacketIO) error {
	return mp.auth.handshakeSecondTime(backendIO, mp.sessionToken)
}

func (mp *mockProxy) processCmd(clientIO, backendIO *pnet.PacketIO) error {
	request, err := clientIO.ReadPacket()
	if err != nil {
		return err
	}
	_, _, err = mp.cmdProcessor.executeCmd(request, clientIO, backendIO, false)
	return err
}
