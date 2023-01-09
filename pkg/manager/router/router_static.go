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

package router

var _ Router = &StaticRouter{}

type StaticRouter struct {
	addrs []string
	cnt   int
}

func NewStaticRouter(addr []string) *StaticRouter {
	return &StaticRouter{addrs: addr}
}

func (r *StaticRouter) GetBackendSelector() BackendSelector {
	return BackendSelector{
		routeOnce: func(excluded []string) string {
			for _, addr := range r.addrs {
				found := false
				for _, e := range excluded {
					if e == addr {
						found = true
						break
					}
				}
				if !found {
					return addr
				}
			}
			return ""
		},
		addConn: func(addr string, conn RedirectableConn) error {
			r.cnt++
			return nil
		},
	}
}

func (r *StaticRouter) RedirectConnections() error {
	return nil
}

func (r *StaticRouter) ConnCount() int {
	return r.cnt
}

func (r *StaticRouter) Close() {
}

func (r *StaticRouter) OnRedirectSucceed(from, to string, conn RedirectableConn) error {
	return nil
}

func (r *StaticRouter) OnRedirectFail(from, to string, conn RedirectableConn) error {
	return nil
}

func (r *StaticRouter) OnConnClosed(addr string, conn RedirectableConn) error {
	r.cnt--
	return nil
}
