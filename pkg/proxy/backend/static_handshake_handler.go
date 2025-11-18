// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiproxy/pkg/balance/router"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
)

// StaticHandshakeHandler always returns a static router.
var _ HandshakeHandler = (*StaticHandshakeHandler)(nil)

type StaticHandshakeHandler struct {
	rt router.Router
}

// NewStaticHandshakeHandler creates a StaticHandshakeHandler.
func NewStaticHandshakeHandler(addr string) *StaticHandshakeHandler {
	return &StaticHandshakeHandler{
		rt: router.NewStaticRouter([]string{addr}),
	}
}

func (handler *StaticHandshakeHandler) HandleHandshakeResp(ConnContext, *pnet.HandshakeResp) error {
	return nil
}

func (handler *StaticHandshakeHandler) HandleHandshakeErr(ConnContext, *mysql.MyError) bool {
	return false
}

func (handler *StaticHandshakeHandler) GetRouter(ConnContext, *pnet.HandshakeResp) (router.Router, error) {
	return handler.rt, nil
}

func (handler *StaticHandshakeHandler) OnHandshake(ConnContext, string, error, ErrorSource) {
}

func (handler *StaticHandshakeHandler) OnTraffic(ConnContext) {
}

func (handler *StaticHandshakeHandler) OnConnClose(ConnContext, ErrorSource) error {
	return nil
}

func (handler *StaticHandshakeHandler) GetCapability() pnet.Capability {
	return SupportedServerCapabilities
}

func (handler *StaticHandshakeHandler) GetServerVersion() string {
	return pnet.ServerVersion
}
