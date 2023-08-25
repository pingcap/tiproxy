// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/pkg/manager/namespace"
	"github.com/pingcap/TiProxy/pkg/manager/router"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"go.uber.org/zap"
)

// Context keys.
type ConnContextKey string

const (
	ConnContextKeyTLSState ConnContextKey = "tls-state"
	ConnContextKeyConnID   ConnContextKey = "conn-id"
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
var _ HandshakeHandler = (*CustomHandshakeHandler)(nil)

type ConnContext interface {
	ClientAddr() string
	ServerAddr() string
	ClientInBytes() uint64
	ClientOutBytes() uint64
	QuitSource() ErrorSource
	UpdateLogger(fields ...zap.Field)
	SetValue(key, val any)
	Value(key any) any
}

type HandshakeHandler interface {
	HandleHandshakeResp(ctx ConnContext, resp *pnet.HandshakeResp) error
	GetRouter(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error)
	OnHandshake(ctx ConnContext, to string, err error)
	OnConnClose(ctx ConnContext) error
	OnTraffic(ctx ConnContext)
	GetCapability() pnet.Capability
	GetServerVersion() string
}

type DefaultHandshakeHandler struct {
	nsManager     *namespace.NamespaceManager
	serverVersion string
}

func NewDefaultHandshakeHandler(nsManager *namespace.NamespaceManager, serverVersion string) *DefaultHandshakeHandler {
	return &DefaultHandshakeHandler{
		nsManager:     nsManager,
		serverVersion: serverVersion,
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
	ctx.UpdateLogger(zap.String("ns", ns.Name()))
	return ns.GetRouter(), nil
}

func (handler *DefaultHandshakeHandler) OnHandshake(ConnContext, string, error) {
}

func (handler *DefaultHandshakeHandler) OnTraffic(ConnContext) {
}

func (handler *DefaultHandshakeHandler) OnConnClose(ConnContext) error {
	return nil
}

func (handler *DefaultHandshakeHandler) GetCapability() pnet.Capability {
	return SupportedServerCapabilities
}

func (handler *DefaultHandshakeHandler) GetServerVersion() string {
	if len(handler.serverVersion) > 0 {
		return handler.serverVersion
	}
	// TiProxy sends the server version before getting the router, so we don't know which router to get.
	// Just get the default one.
	if ns, ok := handler.nsManager.GetNamespace("default"); ok {
		if rt := ns.GetRouter(); rt != nil {
			if serverVersion := rt.ServerVersion(); len(serverVersion) > 0 {
				return serverVersion
			}
		}
	}
	return pnet.ServerVersion
}

type CustomHandshakeHandler struct {
	getRouter           func(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error)
	onHandshake         func(ConnContext, string, error)
	onTraffic           func(ConnContext)
	onConnClose         func(ConnContext) error
	handleHandshakeResp func(ctx ConnContext, resp *pnet.HandshakeResp) error
	getCapability       func() pnet.Capability
	getServerVersion    func() string
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

func (h *CustomHandshakeHandler) OnTraffic(ctx ConnContext) {
	if h.onTraffic != nil {
		h.onTraffic(ctx)
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

func (h *CustomHandshakeHandler) GetServerVersion() string {
	if h.getServerVersion != nil {
		return h.getServerVersion()
	}
	return pnet.ServerVersion
}
