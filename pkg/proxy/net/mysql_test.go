// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandshakeResp(t *testing.T) {
	resp1 := &HandshakeResp{
		Attrs:      map[string]string{"key": "value"},
		User:       "user",
		DB:         "db",
		AuthPlugin: "plugin",
		AuthData:   []byte("1234567890"),
		Capability: ^ClientPluginAuthLenencClientData,
		Collation:  0,
	}
	b := MakeHandshakeResponse(resp1)
	resp2, err := ParseHandshakeResponse(b)
	require.Equal(t, resp1, resp2)
	require.NoError(t, err)
}

func TestChangeUserReq(t *testing.T) {
	req1 := &ChangeUserReq{
		Attrs:      map[string]string{"key": "value"},
		User:       "user",
		DB:         "db",
		AuthPlugin: "plugin",
		AuthData:   []byte("1234567890"),
		Charset:    []byte{0x11, 0x22},
	}
	capability := ClientConnectAttrs | ClientSecureConnection | ClientPluginAuth
	b := MakeChangeUser(req1, capability)
	req2, err := ParseChangeUser(b, capability)
	require.NoError(t, err)
	require.Equal(t, req1, req2)

	capability = 0
	req1.Attrs = nil
	b = MakeChangeUser(req1, capability)
	req2, err = ParseChangeUser(b, capability)
	require.NoError(t, err)
}
