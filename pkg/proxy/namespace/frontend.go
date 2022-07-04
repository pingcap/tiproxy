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

type SQLInfo struct {
	SQL string
}

type FrontendNamespace struct {
	allowedDBs   []string
	allowedDBSet map[string]struct{}
	usernames    []string
	sqlBlacklist map[uint32]SQLInfo
	sqlWhitelist map[uint32]SQLInfo
}

func (n *FrontendNamespace) IsDatabaseAllowed(db string) bool {
	_, ok := n.allowedDBSet[db]
	return ok
}

func (n *FrontendNamespace) ListDatabases() []string {
	ret := make([]string, len(n.allowedDBs))
	copy(ret, n.allowedDBs)
	return ret
}

func (n *FrontendNamespace) IsDeniedSQL(sqlFeature uint32) bool {
	_, ok := n.sqlBlacklist[sqlFeature]
	return ok
}

func (n *FrontendNamespace) IsAllowedSQL(sqlFeature uint32) bool {
	_, ok := n.sqlWhitelist[sqlFeature]
	return ok
}
