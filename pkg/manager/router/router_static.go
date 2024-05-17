// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import "sync/atomic"

var _ Router = &StaticRouter{}

type StaticRouter struct {
	backends []*StaticBackend
	cnt      int
}

func NewStaticRouter(addrs []string) *StaticRouter {
	backends := make([]*StaticBackend, 0, len(addrs))
	for _, addr := range addrs {
		backends = append(backends, NewStaticBackend(addr))
	}
	return &StaticRouter{backends: backends}
}

func (r *StaticRouter) GetBackendSelector() BackendSelector {
	return BackendSelector{
		routeOnce: func(excluded []BackendInst) (BackendInst, error) {
			for _, backend := range r.backends {
				found := false
				for _, e := range excluded {
					if e.Addr() == backend.Addr() {
						found = true
						break
					}
				}
				if !found {
					return backend, nil
				}
			}
			return nil, ErrNoBackend
		},
		onCreate: func(backend BackendInst, conn RedirectableConn, succeed bool) {
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

func (r *StaticRouter) InZeroBackendMode() bool {
	return false
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

type StaticBackend struct {
	addr    string
	healthy atomic.Bool
}

func NewStaticBackend(addr string) *StaticBackend {
	backend := &StaticBackend{
		addr: addr,
	}
	backend.healthy.Store(true)
	return backend
}

func (b *StaticBackend) Addr() string {
	return b.addr
}

func (b *StaticBackend) Healthy() bool {
	return b.healthy.Load()
}

func (b *StaticBackend) SetHealthy(healthy bool) {
	b.healthy.Store(healthy)
}
