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
	"github.com/djshow832/weir/pkg/util/sync2"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type NamespaceManager struct {
	switchIndex sync2.BoolIndex
	users       [2]*UserNamespaceMapper
	nss         [2]*NamespaceHolder

	reloadLock     sync.Mutex
	reloadPrepared map[string]bool
}

func CreateNamespaceManager(cfgs []*config.Namespace) (*NamespaceManager, error) {
	users, err := CreateUserNamespaceMapper(cfgs)
	if err != nil {
		return nil, errors.WithMessage(err, "create UserNamespaceMapper error")
	}

	nss, err := CreateNamespaceHolder(cfgs)
	if err != nil {
		return nil, errors.WithMessage(err, "create NamespaceHolder error")
	}

	mgr := NewNamespaceManager(users, nss)
	return mgr, nil
}

func NewNamespaceManager(users *UserNamespaceMapper, nss *NamespaceHolder) *NamespaceManager {
	mgr := &NamespaceManager{
		reloadPrepared: make(map[string]bool),
	}
	mgr.users[0] = users
	mgr.nss[0] = nss
	return mgr
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
	n.reloadLock.Lock()
	defer n.reloadLock.Unlock()

	newUsers := n.getCurrentUsers().Clone()
	newUsers.RemoveNamespaceUsers(namespace)
	if err := newUsers.AddNamespaceUsers(namespace, &cfg.Frontend); err != nil {
		return errors.WithMessage(err, "add namespace users error")
	}

	newNs, err := BuildNamespace(cfg)
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
	n.reloadLock.Lock()
	defer n.reloadLock.Unlock()

	for _, namespace := range namespaces {
		if !n.reloadPrepared[namespace] {
			return errors.Errorf("namespace is not prepared: %s", namespace)
		}
	}

	n.toggle()
	return nil
}

func (n *NamespaceManager) RemoveNamespace(name string) {
	n.reloadLock.Lock()
	defer n.reloadLock.Unlock()

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
	current, _, _ := n.switchIndex.Get()
	return n.users[current], n.nss[current]
}

func (n *NamespaceManager) getCurrentUsers() *UserNamespaceMapper {
	current, _, _ := n.switchIndex.Get()
	return n.users[current]
}

func (n *NamespaceManager) getCurrentNamespaces() *NamespaceHolder {
	current, _, _ := n.switchIndex.Get()
	return n.nss[current]
}

func (n *NamespaceManager) setOther(users *UserNamespaceMapper, nss *NamespaceHolder) {
	_, other, _ := n.switchIndex.Get()
	n.users[other] = users
	n.nss[other] = nss
}

func (n *NamespaceManager) toggle() {
	_, _, currentFlag := n.switchIndex.Get()
	n.switchIndex.Set(!currentFlag)
}

func (n *NamespaceManager) closeNamespace(ns Namespace) error {
	return nil
}

func (n *NamespaceManager) Close() error {
	return nil
}
