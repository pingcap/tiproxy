// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
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
	_, err = ParseChangeUser(b, capability)
	require.NoError(t, err)
}

func TestLogAttrs(t *testing.T) {
	attrs := map[string]string{
		AttrNameClientVersion: "8.1.0",
		AttrNameClientName1:   "libmysql",
		AttrNameProgramName:   "mysql",
	}
	lg, text := logger.CreateLoggerForTest(t)
	lg.Info("connection info", Attr2ZapFields(attrs)...)
	str := text.String()
	require.Contains(t, str, `"client_version": "8.1.0"`)
	require.Contains(t, str, `"client_name": "libmysql"`)
	require.Contains(t, str, `"program_name": "mysql"`)
}

func TestMySQLError(t *testing.T) {
	myerr := &gomysql.MyError{}
	require.True(t, IsMySQLError(errors.Wrap(ErrHandshakeTLS, myerr)))
	require.False(t, IsMySQLError(errors.Wrap(myerr, ErrHandshakeTLS)))
	require.False(t, IsMySQLError(ErrHandshakeTLS))
	require.True(t, errors.Is(errors.Wrap(ErrHandshakeTLS, myerr), ErrHandshakeTLS))
	require.True(t, errors.Is(errors.Wrap(myerr, ErrHandshakeTLS), ErrHandshakeTLS))
}
