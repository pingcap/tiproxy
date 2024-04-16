// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/balance/router"
	"github.com/pingcap/tiproxy/pkg/manager/namespace"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"go.uber.org/zap"
)

// Interfaces in this file are used for the serverless tier.

// Context keys.
type ConnContextKey string

const (
	ConnContextKeyTLSState ConnContextKey = "tls-state"
	ConnContextKeyConnID   ConnContextKey = "conn-id"
	ConnContextKeyConnAddr ConnContextKey = "conn-addr"
)

var _ HandshakeHandler = (*DefaultHandshakeHandler)(nil)
var _ HandshakeHandler = (*CustomHandshakeHandler)(nil)

// ConnContext saves the connection attributes that are read by HandshakeHandler.
// These interfaces should not request for locks because HandshakeHandler already holds the lock.
type ConnContext interface {
	ClientAddr() string
	ServerAddr() string
	ClientInBytes() uint64
	ClientOutBytes() uint64
	UpdateLogger(fields ...zap.Field)
	SetValue(key, val any)
	Value(key any) any
}

// HandshakeHandler contains the hooks that are called during the connection lifecycle.
// All the interfaces should be called within a lock so that the interfaces of ConnContext are thread-safe.
type HandshakeHandler interface {
	HandleHandshakeResp(ctx ConnContext, resp *pnet.HandshakeResp) error
	HandleHandshakeErr(ctx ConnContext, err *mysql.MyError) bool // return true means retry connect
	GetRouter(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error)
	OnHandshake(ctx ConnContext, to string, err error, src ErrorSource)
	OnConnClose(ctx ConnContext, src ErrorSource) error
	OnTraffic(ctx ConnContext)
	GetCapability() pnet.Capability
	GetServerVersion() string
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

func (handler *DefaultHandshakeHandler) HandleHandshakeErr(ctx ConnContext, err *mysql.MyError) bool {
	return false
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

func (handler *DefaultHandshakeHandler) OnHandshake(ConnContext, string, error, ErrorSource) {
}

func (handler *DefaultHandshakeHandler) OnTraffic(ConnContext) {
}

func (handler *DefaultHandshakeHandler) OnConnClose(ConnContext, ErrorSource) error {
	return nil
}

func (handler *DefaultHandshakeHandler) GetCapability() pnet.Capability {
	return SupportedServerCapabilities
}

func (handler *DefaultHandshakeHandler) GetServerVersion() string {
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
	onHandshake         func(ConnContext, string, error, ErrorSource)
	onTraffic           func(ConnContext)
	onConnClose         func(ConnContext, ErrorSource) error
	handleHandshakeResp func(ctx ConnContext, resp *pnet.HandshakeResp) error
	handleHandshakeErr  func(ctx ConnContext, err *mysql.MyError) bool
	getCapability       func() pnet.Capability
	getServerVersion    func() string
}

func (h *CustomHandshakeHandler) GetRouter(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error) {
	if h.getRouter != nil {
		return h.getRouter(ctx, resp)
	}
	return nil, errors.New("no router")
}

func (h *CustomHandshakeHandler) OnHandshake(ctx ConnContext, addr string, err error, src ErrorSource) {
	if h.onHandshake != nil {
		h.onHandshake(ctx, addr, err, src)
	}
}

func (h *CustomHandshakeHandler) OnTraffic(ctx ConnContext) {
	if h.onTraffic != nil {
		h.onTraffic(ctx)
	}
}

func (h *CustomHandshakeHandler) OnConnClose(ctx ConnContext, src ErrorSource) error {
	if h.onConnClose != nil {
		return h.onConnClose(ctx, src)
	}
	return nil
}

func (h *CustomHandshakeHandler) HandleHandshakeResp(ctx ConnContext, resp *pnet.HandshakeResp) error {
	if h.handleHandshakeResp != nil {
		return h.handleHandshakeResp(ctx, resp)
	}
	return nil
}

func (h *CustomHandshakeHandler) HandleHandshakeErr(ctx ConnContext, err *mysql.MyError) bool {
	if h.handleHandshakeErr != nil {
		return h.handleHandshakeErr(ctx, err)
	}
	return false
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
