// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"time"

	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"go.uber.org/zap"
)

const (
	// slowExecMinLatency is the minimum replay latency to consider a command slow.
	slowExecMinLatency = 500 * time.Millisecond
	// slowExecRatio triggers a slow-exec log when replay latency exceeds audit duration by this factor.
	slowExecRatio = 10.0
	// slowConnectLatency logs backend connect/handshake slower than this threshold.
	slowConnectLatency = time.Second
	// connQueueWarnStep logs when a connection queue grows past multiples of this size.
	connQueueWarnStep = 64
	// maxSQLLogLen limits SQL text length in diagnostic logs.
	maxSQLLogLen = 512
)

func isSlowExec(replayLatency, auditLatency time.Duration) bool {
	if replayLatency < slowExecMinLatency {
		return false
	}
	if auditLatency <= 0 {
		return true
	}
	return float64(replayLatency) >= float64(auditLatency)*slowExecRatio
}

func updateMaxConnQueue(stats *ReplayStats, queueLen int) {
	if stats == nil || queueLen <= 0 {
		return
	}
	queue := int64(queueLen)
	for {
		cur := stats.MaxConnQueue.Load()
		if queue <= cur {
			return
		}
		if stats.MaxConnQueue.CompareAndSwap(cur, queue) {
			return
		}
	}
}

func (c *conn) maybeLogQueueBacklog(queueLen int) {
	if queueLen < connQueueWarnStep {
		return
	}
	level := queueLen / connQueueWarnStep
	if level <= c.lastQueueWarnLevel {
		return
	}
	c.lastQueueWarnLevel = level
	c.lg.Warn("connection command queue backlog",
		zap.Int("queue_len", queueLen),
		zap.Int64("global_pending_cmds", c.replayStats.PendingCmds.Load()),
	)
}

func (c *conn) maybeLogSlowConnect(latency time.Duration, dbName string, err error) {
	if latency < slowConnectLatency {
		return
	}
	fields := []zap.Field{
		zap.Duration("connect_latency", latency),
		zap.String("db", dbName),
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	c.lg.Warn("slow backend connect", fields...)
	c.replayStats.SlowConnects.Add(1)
}

func (c *conn) maybeLogSlowExec(command *cmd.Command, replayLatency time.Duration) {
	if !isSlowExec(replayLatency, command.AuditDuration()) {
		return
	}
	c.replayStats.SlowExecCmds.Add(1)
	fields := []zap.Field{
		zap.Duration("replay_latency", replayLatency),
		zap.Duration("audit_latency", command.AuditDuration()),
		zap.Stringer("cmd", command.Type),
		zap.Uint64("conn_id", command.ConnID),
	}
	if command.FileName != "" {
		fields = append(fields, zap.String("audit_file", command.FileName))
	}
	if command.Line > 0 {
		fields = append(fields, zap.Int("audit_line", command.Line))
	}
	if sql := truncateSQL(command.QueryText()); sql != "" {
		fields = append(fields, zap.String("sql", sql))
	}
	c.lg.Warn("slow replay execution", fields...)
}

func truncateSQL(sql string) string {
	if len(sql) <= maxSQLLogLen {
		return sql
	}
	return sql[:maxSQLLogLen] + "..."
}
