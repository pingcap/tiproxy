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
	"crypto/tls"

	"github.com/pingcap/TiProxy/pkg/proxy/driver"
)

type Namespace struct {
	name             string
	allowedUsernames []string
	allowedDBSet     map[string]struct{}
	router           driver.Router
	frontTLS         *tls.Config
	backTLS          *tls.Config
}

func (n *Namespace) Name() string {
	return n.name
}

func (n *Namespace) FrontTLSConfig() *tls.Config {
	return n.frontTLS
}

func (n *Namespace) BackTLSConfig() *tls.Config {
	return n.backTLS
}

func (n *Namespace) GetRouter() driver.Router {
	return n.router
}

func (n *Namespace) AllowedUsernames() []string {
	return n.allowedUsernames
}

func (n *Namespace) Close() {
	n.router.Close()
}
