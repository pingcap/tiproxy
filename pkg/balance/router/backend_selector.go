// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

type BackendSelector struct {
	excluded  []BackendInst
	cur       BackendInst
	routeOnce func(excluded []BackendInst) (BackendInst, error)
	onCreate  func(backend BackendInst, conn RedirectableConn, succeed bool)
}

func (bs *BackendSelector) Next() (BackendInst, error) {
	backend, err := bs.routeOnce(bs.excluded)
	// If all backends are enumerated, reset and try again.
	if err == ErrNoBackend && len(bs.excluded) > 0 {
		bs.excluded = bs.excluded[:0]
		backend, err = bs.routeOnce(bs.excluded)
	}
	if err != nil {
		return backend, err
	}
	bs.cur = backend
	bs.excluded = append(bs.excluded, backend)
	return backend, nil
}

func (bs *BackendSelector) Finish(conn RedirectableConn, succeed bool) {
	bs.onCreate(bs.cur, conn, succeed)
}
