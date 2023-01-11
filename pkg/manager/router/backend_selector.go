// Copyright 2023 PingCAP, Inc.
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

type BackendSelector struct {
	excluded  []string
	cur       string
	routeOnce func(excluded []string) (string, error)
	addConn   func(addr string, conn RedirectableConn) error
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

func (bs *BackendSelector) Succeed(conn RedirectableConn) error {
	return bs.addConn(bs.cur, conn)
}
