// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/pingcap/tiproxy/pkg/manager/id"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
)

type dbFanout struct {
	multipler    int
	dbNameRegexp *regexp.Regexp
	idMgr        *id.IDManager
	replicaConns map[uint64][]uint64
}

func newDBFanout(cfg ReplayConfig, idMgr *id.IDManager) *dbFanout {
	fanout := &dbFanout{
		multipler:    cfg.DBMultipler,
		idMgr:        idMgr,
		replicaConns: make(map[uint64][]uint64),
	}
	if fanout.multipler <= 1 {
		fanout.multipler = 1
		return fanout
	}
	if len(cfg.DBNamePattern) > 0 {
		fanout.dbNameRegexp = regexp.MustCompile(cfg.DBNamePattern)
	}
	return fanout
}

func (f *dbFanout) Expand(command *cmd.Command) ([]*cmd.Command, error) {
	if command == nil || f == nil || f.multipler <= 1 {
		return []*cmd.Command{command}, nil
	}

	replicaConnIDs := f.replicaConnIDs(command.ConnID)

	sqlVariants, preparedStmtVariants := f.buildSQLVariants(command)
	commands := make([]*cmd.Command, 0, len(replicaConnIDs))
	for i, connID := range replicaConnIDs {
		cloned := cloneCommand(command)
		cloned.ConnID = connID
		if command.CurDB != "" {
			cloned.CurDB = targetDBName(command.CurDB, i+1)
		}
		if len(sqlVariants) > 0 {
			cloned.Payload = pnet.MakeQueryPacket(sqlVariants[i])
		}
		if len(preparedStmtVariants) > 0 {
			cloned.PreparedStmt = preparedStmtVariants[i]
			if cloned.Type == pnet.ComStmtPrepare {
				cloned.Payload = pnet.MakePrepareStmtRequest(cloned.PreparedStmt)
			}
		}
		if cloned.Type == pnet.ComInitDB && cloned.CurDB != "" {
			cloned.Payload = pnet.MakeInitDBRequest(cloned.CurDB)
		}
		commands = append(commands, cloned)
	}
	if command.Type == pnet.ComQuit {
		delete(f.replicaConns, command.ConnID)
	}
	return commands, nil
}

func (f *dbFanout) replicaConnIDs(connID uint64) []uint64 {
	replicaConnIDs, ok := f.replicaConns[connID]
	if ok {
		return replicaConnIDs
	}

	replicaConnIDs = make([]uint64, f.multipler)
	for i := 0; i < f.multipler; i++ {
		replicaConnIDs[i] = f.idMgr.NewID()
	}
	f.replicaConns[connID] = replicaConnIDs
	return replicaConnIDs
}

func cloneCommand(command *cmd.Command) *cmd.Command {
	cloned := *command
	cloned.Payload = slices.Clone(command.Payload)
	cloned.Params = append([]any(nil), command.Params...)
	return &cloned
}

func targetDBName(db string, replica int) string {
	if replica <= 1 || len(db) == 0 {
		return db
	}
	return fmt.Sprintf("%s_%d", db, replica)
}

func (f *dbFanout) buildSQLVariants(command *cmd.Command) ([]string, []string) {
	if f.dbNameRegexp == nil || command.CurDB == "" {
		return nil, nil
	}

	var sqlVariants []string
	if command.Type == pnet.ComQuery && len(command.Payload) > 1 {
		sqlVariants = f.rewriteVariants(string(command.Payload[1:]))
	}

	var preparedStmtVariants []string
	preparedStmt := command.PreparedStmt
	if len(preparedStmt) == 0 && command.Type == pnet.ComStmtPrepare && len(command.Payload) > 1 {
		preparedStmt = string(command.Payload[1:])
	}
	if len(preparedStmt) > 0 {
		preparedStmtVariants = f.rewriteVariants(preparedStmt)
	}

	return sqlVariants, preparedStmtVariants
}

func (f *dbFanout) rewriteVariants(sql string) []string {
	if len(sql) == 0 || f.dbNameRegexp == nil {
		return nil
	}

	matches := f.dbNameRegexp.FindAllStringIndex(sql, -1)
	if len(matches) == 0 {
		return nil
	}

	variants := make([]string, f.multipler)
	variants[0] = sql
	for replica := 2; replica <= f.multipler; replica++ {
		suffix := fmt.Sprintf("_%d", replica)
		var builder strings.Builder
		builder.Grow(len(sql) + len(matches)*len(suffix))
		last := 0
		for _, match := range matches {
			builder.WriteString(sql[last:match[1]])
			builder.WriteString(suffix)
			last = match[1]
		}
		builder.WriteString(sql[last:])
		variants[replica-1] = builder.String()
	}
	return variants
}
