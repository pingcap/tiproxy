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
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/pkg/manager/namespace"
	"github.com/pingcap/TiProxy/pkg/manager/router"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
)

// Context keys.
type ConnContextKey string

const (
	ConnContextKeyTLSState ConnContextKey = "tls-state"
)

type ErrorSource int

const (
	// SrcClientQuit includes: client quit; bad client conn
	SrcClientQuit ErrorSource = iota
	// SrcClientErr includes: wrong password; mal format packet
	SrcClientErr
	// SrcProxyQuit includes: proxy graceful shutdown
	SrcProxyQuit
	// SrcProxyErr includes: cannot get backend list; capability negotiation
	SrcProxyErr
	// SrcBackendQuit includes: backend quit
	SrcBackendQuit
	// SrcBackendErr is reserved
	SrcBackendErr
)

func (es ErrorSource) String() string {
	switch es {
	case SrcClientQuit:
		return "client quit"
	case SrcClientErr:
		return "client error"
	case SrcProxyQuit:
		return "proxy shutdown"
	case SrcProxyErr:
		return "proxy error"
	case SrcBackendQuit:
		return "backend quit"
	case SrcBackendErr:
		return "backend error"
	}
	return "unknown"
}

var _ HandshakeHandler = (*DefaultHandshakeHandler)(nil)

type ConnContext interface {
	ClientAddr() string
	ServerAddr() string
	ClientInBytes() uint64
	ClientOutBytes() uint64
	QuitSource() ErrorSource
	SetValue(key, val any)
	Value(key any) any
}

type HandshakeHandler interface {
	HandleHandshakeResp(ctx ConnContext, resp *pnet.HandshakeResp) error
	GetRouter(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error)
	OnHandshake(ctx ConnContext, to string, err error)
	OnConnClose(ctx ConnContext) error
	GetCapability() pnet.Capability
}

type DefaultHandshakeHandler struct {
	nsManager *namespace.NamespaceManager
}

func NewDefaultHandshakeHandler(nsManager *namespace.NamespaceManager) *DefaultHandshakeHandler {
	return &DefaultHandshakeHandler{
		nsManager: nsManager,
	}
}

func (handler *DefaultHandshakeHandler) HandleHandshakeResp(ConnContext, *pnet.HandshakeResp) error {
	return nil
}

func (handler *DefaultHandshakeHandler) GetRouter(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error) {
	ns, ok := handler.nsManager.GetNamespaceByUser(resp.User)
	if !ok {
		ns, ok = handler.nsManager.GetNamespace("default")
	}
	if !ok {
		return nil, errors.New("failed to find a namespace")
	}
	return ns.GetRouter(), nil
}

func (handler *DefaultHandshakeHandler) OnHandshake(ConnContext, string, error) {
}

func (handler *DefaultHandshakeHandler) OnConnClose(ConnContext) error {
	return nil
}

func (handler *DefaultHandshakeHandler) GetCapability() pnet.Capability {
	return SupportedServerCapabilities
}

type CustomHandshakeHandler struct {
	getRouter           func(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error)
	onHandshake         func(ConnContext, string, error)
	onConnClose         func(ConnContext) error
	handleHandshakeResp func(ctx ConnContext, resp *pnet.HandshakeResp) error
	getCapability       func() pnet.Capability
}

func (h *CustomHandshakeHandler) GetRouter(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error) {
	if h.getRouter != nil {
		return h.getRouter(ctx, resp)
	}
	return nil, errors.New("no router")
}

func (h *CustomHandshakeHandler) OnHandshake(ctx ConnContext, addr string, err error) {
	if h.onHandshake != nil {
		h.onHandshake(ctx, addr, err)
	}
}

func (h *CustomHandshakeHandler) OnConnClose(ctx ConnContext) error {
	if h.onConnClose != nil {
		return h.onConnClose(ctx)
	}
	return nil
}

func (h *CustomHandshakeHandler) HandleHandshakeResp(ctx ConnContext, resp *pnet.HandshakeResp) error {
	if h.handleHandshakeResp != nil {
		return h.handleHandshakeResp(ctx, resp)
	}
	return nil
}

func (h *CustomHandshakeHandler) GetCapability() pnet.Capability {
	if h.getCapability != nil {
		return h.getCapability()
	}
	return SupportedServerCapabilities
}
