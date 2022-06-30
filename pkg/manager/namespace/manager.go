// Copyright 2020 Ipalfish, Inc.
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

package namespace

import (
	"sync"

	"github.com/djshow832/weir/pkg/config"
	"github.com/djshow832/weir/pkg/proxy/driver"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type NamespaceManager struct {
	sync.RWMutex

	switchIndex    int
	users          [2]*UserNamespaceMapper
	nss            [2]*NamespaceHolder
	reloadPrepared map[string]bool

	client *clientv3.Client
}

func NewNamespaceManager() *NamespaceManager {
	return &NamespaceManager{}
}

func (mgr *NamespaceManager) Init(cfgs []*config.Namespace, client *clientv3.Client) error {
	mgr.Lock()
	defer mgr.Unlock()

	users, err := CreateUserNamespaceMapper(cfgs)
	if err != nil {
		return errors.WithMessage(err, "create UserNamespaceMapper error")
	}

	nss, err := CreateNamespaceHolder(cfgs, client)
	if err != nil {
		return errors.WithMessage(err, "create NamespaceHolder error")
	}

	mgr.reloadPrepared = make(map[string]bool)
	mgr.client = client
	mgr.users[0] = users
	mgr.nss[0] = nss
	return nil
}

func (n *NamespaceManager) Auth(username string, pwd, salt []byte) (driver.Namespace, bool) {
	nsName, ok := n.getNamespaceByUsername(username)
	if !ok {
		return nil, false
	}

	wrapper := &NamespaceWrapper{
		nsmgr: n,
		name:  nsName,
	}

	return wrapper, true
}

func (n *NamespaceManager) RedirectConnections() error {
	return n.getCurrentNamespaces().RedirectConnections()
}

func (n *NamespaceManager) PrepareReloadNamespace(namespace string, cfg *config.Namespace) error {
	n.Lock()
	defer n.Unlock()

	newUsers := n.getCurrentUsers().Clone()
	newUsers.RemoveNamespaceUsers(namespace)
	if err := newUsers.AddNamespaceUsers(namespace, &cfg.Frontend); err != nil {
		return errors.WithMessage(err, "add namespace users error")
	}

	newNs, err := BuildNamespace(cfg, n.client)
	if err != nil {
		return errors.WithMessage(err, "build namespace error")
	}

	newNss := n.getCurrentNamespaces().Clone()
	newNss.Set(namespace, newNs)

	n.setOther(newUsers, newNss)
	n.reloadPrepared[namespace] = true

	return nil
}

func (n *NamespaceManager) CommitReloadNamespaces(namespaces []string) error {
	n.Lock()
	defer n.Unlock()

	for _, namespace := range namespaces {
		if !n.reloadPrepared[namespace] {
			return errors.Errorf("namespace is not prepared: %s", namespace)
		}
	}

	n.toggle()
	return nil
}

func (n *NamespaceManager) RemoveNamespace(name string) {
	n.Lock()
	defer n.Unlock()

	n.getCurrentUsers().RemoveNamespaceUsers(name)
	nss := n.getCurrentNamespaces()
	ns, ok := nss.Get(name)
	if !ok {
		return
	}

	if err := n.closeNamespace(ns); err != nil {
		logutil.BgLogger().Error("remove namespace error", zap.Error(err), zap.String("namespace", name))
		return
	}

	nss.Delete(name)
}

func (n *NamespaceManager) getNamespaceByUsername(username string) (string, bool) {
	return n.getCurrentUsers().GetUserNamespace(username)
}

func (n *NamespaceManager) getCurrent() (*UserNamespaceMapper, *NamespaceHolder) {
	return n.users[n.switchIndex], n.nss[n.switchIndex]
}

func (n *NamespaceManager) getCurrentUsers() *UserNamespaceMapper {
	return n.users[n.switchIndex]
}

func (n *NamespaceManager) getCurrentNamespaces() *NamespaceHolder {
	return n.nss[n.switchIndex]
}

func (n *NamespaceManager) getOtherIndex() int {
	if n.switchIndex == 0 {
		return 1
	} else {
		return 0
	}
}

func (n *NamespaceManager) setOther(users *UserNamespaceMapper, nss *NamespaceHolder) {
	other := n.getOtherIndex()
	n.users[other], n.nss[other] = users, nss
}

func (n *NamespaceManager) toggle() {
	n.switchIndex = n.getOtherIndex()
}

func (n *NamespaceManager) closeNamespace(ns Namespace) error {
	return nil
}

func (n *NamespaceManager) Close() error {
	return nil
}
