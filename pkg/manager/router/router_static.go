// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

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
		routeOnce: func(excluded []string) (string, error) {
			for _, addr := range r.addrs {
				found := false
				for _, e := range excluded {
					if e == addr {
						found = true
						break
					}
				}
				if !found {
					return addr, nil
				}
			}
			return "", nil
		},
		onCreate: func(addr string, conn RedirectableConn, succeed bool) {
			if succeed {
				r.cnt++
			}
		},
	}
}

func (r *StaticRouter) RefreshBackend() {}

func (r *StaticRouter) RedirectConnections() error {
	return nil
}

func (r *StaticRouter) ConnCount() int {
	return r.cnt
}

func (r *StaticRouter) ServerVersion() string {
	return ""
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
