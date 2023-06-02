// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

type BackendSelector struct {
	excluded  []string
	cur       string
	routeOnce func(excluded []string) (string, error)
	onCreate  func(addr string, conn RedirectableConn, succeed bool)
}

func (bs *BackendSelector) Reset() {
	bs.excluded = bs.excluded[:0]
}

func (bs *BackendSelector) Next() (string, error) {
	addr, err := bs.routeOnce(bs.excluded)
	if err != nil {
		return addr, err
	}
	bs.cur = addr
	if bs.cur != "" {
		bs.excluded = append(bs.excluded, bs.cur)
	}
	return bs.cur, nil
}

func (bs *BackendSelector) Finish(conn RedirectableConn, succeed bool) {
	bs.onCreate(bs.cur, conn, succeed)
}
