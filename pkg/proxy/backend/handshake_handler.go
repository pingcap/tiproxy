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
var _ HandshakeHandler = (*DefaultHandshakeHandler)(nil)

type ConnContext interface {
	ClientAddr() string
	ServerAddr() string
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
