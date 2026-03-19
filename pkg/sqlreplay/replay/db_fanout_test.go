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
		DBNamePattern: `ec_force_app_[a-z]+_db`,
	}, id.NewIDManager())
	command := &cmd.Command{
		ConnID:  1001,
		Type:    pnet.ComQuery,
		Payload: pnet.MakeQueryPacket("select * from ec_force_app_abc_db.t1"),
		CurDB:   "ec_force_app_abc_db",
	}

	expanded, err := fanout.Expand(command)
	require.NoError(t, err)
	require.Len(t, expanded, 4)

	connIDs := make(map[uint64]struct{}, len(expanded))
	for i, cloned := range expanded {
		connIDs[cloned.ConnID] = struct{}{}
		expectedDB := targetDBName("ec_force_app_abc_db", i+1)
		require.Equal(t, expectedDB, cloned.CurDB)
		require.Equal(t, fmt.Sprintf("select * from %s.t1", expectedDB), hack.String(cloned.Payload[1:]))
	}
	require.Len(t, connIDs, 4)

	second, err := fanout.Expand(&cmd.Command{
		ConnID:       1001,
		Type:         pnet.ComStmtPrepare,
		PreparedStmt: "select * from ec_force_app_abc_db.t1 where id = ?",
		Payload:      pnet.MakePrepareStmtRequest("select * from ec_force_app_abc_db.t1 where id = ?"),
		CurDB:        "ec_force_app_abc_db",
	})
	require.NoError(t, err)
	require.Len(t, second, 4)
	for i := range second {
		require.Equal(t, expanded[i].ConnID, second[i].ConnID)
		expectedDB := targetDBName("ec_force_app_abc_db", i+1)
		require.Equal(t, fmt.Sprintf("select * from %s.t1 where id = ?", expectedDB), second[i].PreparedStmt)
		require.Equal(t, second[i].PreparedStmt, hack.String(second[i].Payload[1:]))
	}
}

func TestDBFanoutExpandWithoutCurrentDB(t *testing.T) {
	fanout := newDBFanout(ReplayConfig{
		DBMultipler:   3,
		DBNamePattern: `ec_force_app_[a-z]+_db`,
	}, id.NewIDManager())
	command := &cmd.Command{
		ConnID:  2001,
		Type:    pnet.ComQuery,
		Payload: pnet.MakeQueryPacket("select * from ec_force_app_abc_db.t1"),
	}

	expanded, err := fanout.Expand(command)
	require.NoError(t, err)
	require.Len(t, expanded, 3)
	for _, cloned := range expanded {
		require.Empty(t, cloned.CurDB)
		require.Equal(t, "select * from ec_force_app_abc_db.t1", hack.String(cloned.Payload[1:]))
	}
}

func TestReplayWithDBMultipler(t *testing.T) {
	replayer := NewReplay(zap.NewNop(), id.NewIDManager())
	defer replayer.Close()

	line := `[2025/09/14 16:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17573373891] [TIMESTAMP=2025/09/14 16:16:29.585 +08:00] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select * from ec_force_app_abc_db.t1"] [ROWS=0] [CONNECTION_ID=1001] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=ec_force_app_abc_db] [EVENT=COMPLETED]`
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
		DBNamePattern:   `ec_force_app_[a-z]+_db`,
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
		expectedDB := targetDBName("ec_force_app_abc_db", i)
		require.Equal(t, fmt.Sprintf("select * from %s.t1", expectedDB), actual[expectedDB])
	}
}
