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
	"github.com/pingcap/TiProxy/pkg/config"
)

type UserNamespaceMapper struct {
	userToNamespace map[string]string
}

func CreateUserNamespaceMapper(namespaces []*config.Namespace) (*UserNamespaceMapper, error) {
	mapper := make(map[string]string)
	ret := &UserNamespaceMapper{userToNamespace: mapper}
	return ret, nil
}

func (u *UserNamespaceMapper) GetUserNamespace(username string) (string, bool) {
	if username == "" {
		for _, ns := range u.userToNamespace {
			return ns, true
		}
	}

	ns, ok := u.userToNamespace[username]
	return ns, ok
}

func (u *UserNamespaceMapper) Clone() *UserNamespaceMapper {
	ret := make(map[string]string)
	for k, v := range u.userToNamespace {
		ret[k] = v
	}
	return &UserNamespaceMapper{userToNamespace: ret}
}

func (u *UserNamespaceMapper) RemoveNamespaceUsers(ns string) {
	for k, namespace := range u.userToNamespace {
		if ns == namespace {
			delete(u.userToNamespace, k)
		}
	}
}

func (u *UserNamespaceMapper) AddNamespaceUsers(ns string, cfg *config.FrontendNamespace) error {
	return nil
}
