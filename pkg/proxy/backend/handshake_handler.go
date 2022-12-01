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
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
)

var _ HandshakeHandler = (*DefaultHandshakeHandler)(nil)

type HandshakeHandler interface {
	HandleHandshakeResp(resp *pnet.HandshakeResp, sourceAddr string) error
	GetCapability() pnet.Capability
	GetNamespace(nsMgr *namespace.NamespaceManager, resp *pnet.HandshakeResp) (*namespace.Namespace, error)
}

type DefaultHandshakeHandler struct {
}

func NewDefaultHandshakeHandler() *DefaultHandshakeHandler {
	return &DefaultHandshakeHandler{}
}

func (handler *DefaultHandshakeHandler) HandleHandshakeResp(*pnet.HandshakeResp, string) error {
	return nil
}

func (handler *DefaultHandshakeHandler) GetCapability() pnet.Capability {
	return SupportedServerCapabilities
}

func (handler *DefaultHandshakeHandler) GetNamespace(nsMgr *namespace.NamespaceManager, resp *pnet.HandshakeResp) (*namespace.Namespace, error) {
	ns, ok := nsMgr.GetNamespaceByUser(resp.User)
	if !ok {
		ns, ok = nsMgr.GetNamespace("default")
	}
	if !ok {
		return nil, errors.New("failed to find a namespace")
	}
	return ns, nil
}
