// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"net"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/testkit"
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
	myerr := &mysql.MyError{}
	require.True(t, IsMySQLError(errors.Wrap(myerr, ErrHandshakeTLS)))
	require.False(t, IsMySQLError(errors.Wrap(ErrHandshakeTLS, myerr)))
	require.False(t, IsMySQLError(ErrHandshakeTLS))
	require.True(t, errors.Is(errors.Wrap(ErrHandshakeTLS, myerr), ErrHandshakeTLS))
	require.True(t, errors.Is(errors.Wrap(myerr, ErrHandshakeTLS), ErrHandshakeTLS))
}

func TestCheckSqlPort(t *testing.T) {
	// normal
	testkit.TestTCPConn(t,
		func(t *testing.T, c net.Conn) {
			err := CheckSqlPort(c)
			require.NoError(t, err)
		},
		func(t *testing.T, c net.Conn) {
			data := []byte{0, 0, 0, 0, 0}
			conn := packet.NewConn(c)
			require.NoError(t, conn.WritePacket(data))
		}, 1)

	// no write
	testkit.TestTCPConn(t,
		func(t *testing.T, c net.Conn) {
			err := CheckSqlPort(c)
			require.Error(t, err)
		},
		func(t *testing.T, c net.Conn) {
		}, 1)

	// write error code
	testkit.TestTCPConn(t,
		func(t *testing.T, c net.Conn) {
			err := CheckSqlPort(c)
			require.Error(t, err)
		},
		func(t *testing.T, c net.Conn) {
			data := []byte{0, 0, 0, 0, 0xff}
			conn := packet.NewConn(c)
			require.NoError(t, conn.WritePacket(data))
		}, 1)
}

func TestPrepareStmts(t *testing.T) {
	args := []any{
		nil,
		"hello",
		byte(10),
		int16(-100),
		int32(-200),
		int64(-300),
		uint16(100),
		uint32(200),
		uint64(300),
		float32(1.1),
		float64(1.2),
		nil,
	}
	expectedTypes := []byte{
		fieldTypeNULL, 0,
		fieldTypeString, 0,
		fieldTypeTiny, 0x80,
		fieldTypeShort, 0,
		fieldTypeLong, 0,
		fieldTypeLongLong, 0,
		fieldTypeShort, 0x80,
		fieldTypeLong, 0x80,
		fieldTypeLongLong, 0x80,
		fieldTypeFloat, 0,
		fieldTypeDouble, 0,
		fieldTypeNULL, 0,
	}

	b := MakePrepareStmtRequest("select ?")
	require.Len(t, b, len("select ?")+1)

	data1, err := MakeExecuteStmtRequest(1, args, true)
	require.NoError(t, err)

	stmtID, pArgs, newParamTypes, err := ParseExecuteStmtRequest(data1, len(args), nil)
	require.NoError(t, err)
	require.Equal(t, uint32(1), stmtID)
	require.EqualValues(t, args, pArgs)
	require.Equal(t, expectedTypes, newParamTypes)

	data2, err := MakeExecuteStmtRequest(1, pArgs, false)
	require.NoError(t, err)
	require.NotEqual(t, data1, data2)

	stmtID, pArgs, newParamTypes, err = ParseExecuteStmtRequest(data1, len(args), newParamTypes)
	require.NoError(t, err)
	require.Equal(t, uint32(1), stmtID)
	require.EqualValues(t, args, pArgs)
	require.Equal(t, expectedTypes, newParamTypes)
}
