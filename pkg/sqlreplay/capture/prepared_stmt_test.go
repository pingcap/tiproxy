// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package capture

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/sessionstates"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/store"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockLineReader struct {
	data    []byte
	curLine int
	curIdx  int
}

func (mr *mockLineReader) ReadLine() ([]byte, string, int, error) {
	if mr.curIdx >= len(mr.data) {
		return nil, "", 0, io.EOF
	}
	idx := bytes.IndexByte(mr.data[mr.curIdx:], '\n')
	if idx == -1 {
		return nil, "", 0, io.EOF
	}
	idx += mr.curIdx
	line := mr.data[mr.curIdx:idx]
	mr.curIdx = idx + 1
	mr.curLine++
	return line, "", mr.curLine, nil
}

func (mr *mockLineReader) Read(data []byte) (string, int, error) {
	n := len(data)
	if mr.curIdx+n > len(mr.data) {
		return "", 0, io.EOF
	}
	copy(data, mr.data[mr.curIdx:mr.curIdx+n])
	mr.curIdx += n
	return "", mr.curLine, nil
}

func (mr *mockLineReader) Close() {
}

func (mr *mockLineReader) String() string {
	return "mockLineReader"
}

func decodeNativeCommands(t *testing.T, data []byte) []*cmd.Command {
	t.Helper()
	decoder := cmd.NewNativeDecoder()
	reader := &mockLineReader{data: data}
	cmds := make([]*cmd.Command, 0)
	for {
		c, err := decoder.Decode(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
		cmds = append(cmds, c)
	}
	return cmds
}

func newTestCapture(t *testing.T) (*capture, *mockWriter) {
	t.Helper()
	cpt := NewCapture(zap.NewNop())
	writer := newMockWriter(store.WriterCfg{})
	cfg := CaptureConfig{
		Output:    t.TempDir(),
		Duration:  10 * time.Second,
		cmdLogger: writer,
		StartTime: time.Now(),
	}
	require.NoError(t, cpt.Start(cfg))
	return cpt, writer
}

func TestCapturePreparedStmtLifecycle(t *testing.T) {
	cpt, writer := newTestCapture(t)
	defer cpt.Close()

	connID := uint64(1)
	cpt.InitConn(time.Now(), connID, "")

	preparePacket := append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("select ?")...)
	cpt.Capture(StmtInfo{
		Request:   preparePacket,
		StmtID:    10,
		StartTime: time.Now(),
		ConnID:    connID,
	})

	execReq, err := pnet.MakeExecuteStmtRequest(10, []any{int64(1)}, true)
	require.NoError(t, err)
	cpt.Capture(StmtInfo{
		Request:   execReq,
		StartTime: time.Now(),
		ConnID:    connID,
	})

	closeReq := pnet.MakeCloseStmtRequest(10)
	cpt.Capture(StmtInfo{
		Request:   closeReq,
		StartTime: time.Now(),
		ConnID:    connID,
	})

	execAfterClose, err := pnet.MakeExecuteStmtRequest(10, []any{int64(2)}, true)
	require.NoError(t, err)
	cpt.Capture(StmtInfo{
		Request:   execAfterClose,
		StartTime: time.Now(),
		ConnID:    connID,
	})

	cpt.Stop(nil)
	cmds := decodeNativeCommands(t, writer.getData())
	require.Len(t, cmds, 4)

	require.Equal(t, pnet.ComStmtPrepare, cmds[0].Type)
	require.Equal(t, uint32(10), cmds[0].CapturedPsID)

	require.Equal(t, pnet.ComStmtExecute, cmds[1].Type)
	require.Equal(t, uint32(10), cmds[1].CapturedPsID)
	require.Equal(t, "select ?", cmds[1].PreparedStmt)

	require.Equal(t, pnet.ComStmtClose, cmds[2].Type)
	require.Equal(t, uint32(10), cmds[2].CapturedPsID)

	require.Equal(t, pnet.ComStmtExecute, cmds[3].Type)
	require.Equal(t, uint32(10), cmds[3].CapturedPsID)
	require.Empty(t, cmds[3].PreparedStmt)
}

func TestCapturePreparedStmtResetConnection(t *testing.T) {
	cpt, writer := newTestCapture(t)
	defer cpt.Close()

	connID := uint64(1)
	cpt.InitConn(time.Now(), connID, "")

	preparePacket := append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("select ?")...)
	cpt.Capture(StmtInfo{
		Request:   preparePacket,
		StmtID:    11,
		StartTime: time.Now(),
		ConnID:    connID,
	})

	cpt.Capture(StmtInfo{
		Request:   []byte{pnet.ComResetConnection.Byte()},
		StartTime: time.Now(),
		ConnID:    connID,
	})

	execReq, err := pnet.MakeExecuteStmtRequest(11, []any{int64(1)}, true)
	require.NoError(t, err)
	cpt.Capture(StmtInfo{
		Request:   execReq,
		StartTime: time.Now(),
		ConnID:    connID,
	})

	cpt.Stop(nil)
	cmds := decodeNativeCommands(t, writer.getData())
	require.Len(t, cmds, 3)

	require.Equal(t, pnet.ComStmtPrepare, cmds[0].Type)
	require.Equal(t, uint32(11), cmds[0].CapturedPsID)
	require.Equal(t, pnet.ComResetConnection, cmds[1].Type)

	require.Equal(t, pnet.ComStmtExecute, cmds[2].Type)
	require.Equal(t, uint32(11), cmds[2].CapturedPsID)
	require.Empty(t, cmds[2].PreparedStmt)
}

func TestCapturePreparedStmtFromSessionStates(t *testing.T) {
	cpt, writer := newTestCapture(t)
	defer cpt.Close()

	connID := uint64(1)
	ss := sessionstates.SessionStates{
		PreparedStmts: map[uint32]*sessionstates.PreparedStmtInfo{
			7: {
				StmtText: "select ?",
			},
		},
	}
	b, err := json.Marshal(ss)
	require.NoError(t, err)
	sql := "SET SESSION_STATES '" + string(b) + "'"

	execReq, err := pnet.MakeExecuteStmtRequest(7, []any{int64(1)}, true)
	require.NoError(t, err)
	cpt.Capture(StmtInfo{
		Request:   execReq,
		StartTime: time.Now(),
		ConnID:    connID,
		InitSession: func() (string, error) {
			return sql, nil
		},
	})

	cpt.Stop(nil)
	cmds := decodeNativeCommands(t, writer.getData())
	require.Len(t, cmds, 2)

	require.Equal(t, pnet.ComQuery, cmds[0].Type)
	require.Contains(t, string(cmds[0].Payload[1:]), "SET SESSION_STATES")

	require.Equal(t, pnet.ComStmtExecute, cmds[1].Type)
	require.Equal(t, uint32(7), cmds[1].CapturedPsID)
	require.Equal(t, "select ?", cmds[1].PreparedStmt)
}
