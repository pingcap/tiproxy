// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/pkg/manager/id"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"github.com/siddontang/go/hack"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestDBFanoutExpandQuery(t *testing.T) {
	fanout := newDBFanout(ReplayConfig{
		DBMultipler:   4,
		DBNamePattern: `test_[a-z]+_db`,
	}, id.NewIDManager())
	command := &cmd.Command{
		ConnID:  1001,
		Type:    pnet.ComQuery,
		Payload: pnet.MakeQueryPacket("select * from test_abc_db.t1"),
		CurDB:   "test_abc_db",
	}

	expanded, err := fanout.Expand(command)
	require.NoError(t, err)
	require.Len(t, expanded, 4)

	connIDs := make(map[uint64]struct{}, len(expanded))
	for i, cloned := range expanded {
		connIDs[cloned.ConnID] = struct{}{}
		expectedDB := targetDBName("test_abc_db", i+1)
		require.Equal(t, expectedDB, cloned.CurDB)
		require.Equal(t, fmt.Sprintf("select * from %s.t1", expectedDB), hack.String(cloned.Payload[1:]))
	}
	require.Len(t, connIDs, 4)

	second, err := fanout.Expand(&cmd.Command{
		ConnID:       1001,
		Type:         pnet.ComStmtPrepare,
		PreparedStmt: "select * from test_abc_db.t1 where id = ?",
		Payload:      pnet.MakePrepareStmtRequest("select * from test_abc_db.t1 where id = ?"),
		CurDB:        "test_abc_db",
	})
	require.NoError(t, err)
	require.Len(t, second, 4)
	for i := range second {
		require.Equal(t, expanded[i].ConnID, second[i].ConnID)
		expectedDB := targetDBName("test_abc_db", i+1)
		require.Equal(t, fmt.Sprintf("select * from %s.t1 where id = ?", expectedDB), second[i].PreparedStmt)
		require.Equal(t, second[i].PreparedStmt, hack.String(second[i].Payload[1:]))
	}
}

func TestDBFanoutExpandWithoutCurrentDB(t *testing.T) {
	fanout := newDBFanout(ReplayConfig{
		DBMultipler:   3,
		DBNamePattern: `test_[a-z]+_db`,
	}, id.NewIDManager())
	command := &cmd.Command{
		ConnID:  2001,
		Type:    pnet.ComQuery,
		Payload: pnet.MakeQueryPacket("select * from test_abc_db.t1"),
	}

	expanded, err := fanout.Expand(command)
	require.NoError(t, err)
	require.Len(t, expanded, 3)
	for i, cloned := range expanded {
		require.Empty(t, cloned.CurDB)
		expectedDB := targetDBName("test_abc_db", i+1)
		require.Equal(t, fmt.Sprintf("select * from %s.t1", expectedDB), hack.String(cloned.Payload[1:]))
	}
}

func TestDBFanoutExpandCreateDatabaseWithoutCurrentDB(t *testing.T) {
	fanout := newDBFanout(ReplayConfig{
		DBMultipler:   4,
		DBNamePattern: `test_[a-z]+_db`,
	}, id.NewIDManager())
	command := &cmd.Command{
		ConnID:   2003,
		Type:     pnet.ComQuery,
		StmtType: "CreateDatabase",
		Payload:  pnet.MakeQueryPacket("create database test_abc_db"),
	}

	expanded, err := fanout.Expand(command)
	require.NoError(t, err)
	require.Len(t, expanded, 4)
	for i, cloned := range expanded {
		require.Empty(t, cloned.CurDB)
		expectedDB := targetDBName("test_abc_db", i+1)
		require.Equal(t, fmt.Sprintf("create database %s", expectedDB), hack.String(cloned.Payload[1:]))
	}
}

func TestDBFanoutExpandInitDBWithoutCurrentDB(t *testing.T) {
	fanout := newDBFanout(ReplayConfig{
		DBMultipler:   3,
		DBNamePattern: `test_[a-z]+_db`,
	}, id.NewIDManager())
	command := &cmd.Command{
		ConnID:  2002,
		Type:    pnet.ComInitDB,
		Payload: pnet.MakeInitDBRequest("test_abc_db"),
	}

	expanded, err := fanout.Expand(command)
	require.NoError(t, err)
	require.Len(t, expanded, 3)
	for _, cloned := range expanded {
		require.Empty(t, cloned.CurDB)
		require.Equal(t, "test_abc_db", hack.String(cloned.Payload[1:]))
	}
}

func TestDBFanoutReleasesReplicaConnsOnQuit(t *testing.T) {
	fanout := newDBFanout(ReplayConfig{
		DBMultipler:   2,
		DBNamePattern: `test_[a-z]+_db`,
	}, id.NewIDManager())

	first, err := fanout.Expand(&cmd.Command{
		ConnID:  3001,
		Type:    pnet.ComQuery,
		Payload: pnet.MakeQueryPacket("select 1"),
		CurDB:   "test_abc_db",
	})
	require.NoError(t, err)
	require.Len(t, first, 2)

	quit, err := fanout.Expand(&cmd.Command{
		ConnID:  3001,
		Type:    pnet.ComQuit,
		Payload: []byte{pnet.ComQuit.Byte()},
		CurDB:   "test_abc_db",
	})
	require.NoError(t, err)
	require.Len(t, quit, 2)
	require.Equal(t, first[0].ConnID, quit[0].ConnID)
	require.Equal(t, first[1].ConnID, quit[1].ConnID)
	_, ok := fanout.replicaConns[3001]
	require.False(t, ok)

	second, err := fanout.Expand(&cmd.Command{
		ConnID:  3001,
		Type:    pnet.ComQuery,
		Payload: pnet.MakeQueryPacket("select 2"),
		CurDB:   "test_abc_db",
	})
	require.NoError(t, err)
	require.Len(t, second, 2)
	require.NotEqual(t, first[0].ConnID, second[0].ConnID)
	require.NotEqual(t, first[1].ConnID, second[1].ConnID)
}

func TestReplayWithDBMultipler(t *testing.T) {
	replayer := NewReplay(zap.NewNop(), id.NewIDManager())
	defer replayer.Close()

	line := `[2025/09/14 16:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17573373891] [TIMESTAMP=2025/09/14 16:16:29.585 +08:00] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select * from test_abc_db.t1"] [ROWS=0] [CONNECTION_ID=1001] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test_abc_db] [EVENT=COMPLETED]`
	loader := newMockNormalLoader()
	loader.write([]byte(line + "\n"))

	cmdCh := make(chan *cmd.Command, 4)
	connCount := 0
	cfg := ReplayConfig{
		Input:           t.TempDir(),
		Username:        "u1",
		Format:          cmd.FormatAuditLogPlugin,
		StartTime:       time.Now(),
		readers:         []cmd.LineReader{loader},
		report:          newMockReport(replayer.exceptionCh),
		PSCloseStrategy: cmd.PSCloseStrategyDirected,
		DBMultipler:     4,
		DBNamePattern:   `test_[a-z]+_db`,
		connCreator: func(connID uint64, _ uint64) conn.Conn {
			connCount++
			return &mockConn{
				connID:  connID,
				cmdCh:   cmdCh,
				closeCh: replayer.closeConnCh,
				closed:  make(chan struct{}, 1),
			}
		},
	}

	require.NoError(t, replayer.Start(cfg, nil, nil, &backend.BCConfig{}))
	replayer.Wait()
	require.Equal(t, 4, connCount)

	actual := make(map[string]string, 4)
	for i := 0; i < 4; i++ {
		command := <-cmdCh
		actual[command.CurDB] = hack.String(command.Payload[1:])
	}
	for i := 1; i <= 4; i++ {
		expectedDB := targetDBName("test_abc_db", i)
		require.Equal(t, fmt.Sprintf("select * from %s.t1", expectedDB), actual[expectedDB])
	}
}
