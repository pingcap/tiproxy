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

package net

import (
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestHandshakeResp(t *testing.T) {
	resp1 := &HandshakeResp{
		Attrs:      map[string]string{"key": "value"},
		User:       "user",
		DB:         "db",
		AuthPlugin: "plugin",
		AuthData:   []byte("1234567890"),
		Capability: ^mysql.ClientPluginAuthLenencClientData,
		Collation:  0,
	}
	b := MakeHandshakeResponse(resp1)
	resp2 := ParseHandshakeResponse(b)
	require.Equal(t, resp1, resp2)
}
