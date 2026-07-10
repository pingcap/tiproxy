// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package sqlrewrite

import (
	"regexp"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/siddontang/go/hack"
)

var (
	tiflashReadHintRE   = regexp.MustCompile(`(?is)/\*\s*\+\s*read_from_storage\s*\(\s*tiflash\s*\[[^\]]*\]\s*\)\s*\*/`)
	ignorePlanCacheRE   = regexp.MustCompile(`(?is)/\*\s*\+\s*ignore_plan_cache\s*\(\s*\)\s*\*/`)
	ignorePlanCacheHint = "/*+ ignore_plan_cache() */"
	shardTableRE        = regexp.MustCompile(`(?i)\bbc_bet_records_\d+\b`)

	// findBetRecordsListSQL is the canonical SQL for BcBetRecordsMapper.findBetRecordsList.
	// Replay digest is computed from this template; shard table suffixes may differ at runtime.
	findBetRecordsListSQL = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
SELECT
  /*+ read_from_storage(tiflash[b]) */
  b.record_id,
  b.order_no,
  b.round_id,
  b.account,
  b.third_user_name,
  b.third_game_code,
  b.site_code,
  b.platform_id,
  b.category_id gameCategoryId,
  b.bet_time,
  b.settle_time,
  b.all_bet,
  b.valid_bet,
  b.net_profit,
  b.after_balance,
  b.tax,
  b.rake,
  b.insurance,
  b.props,
  b.settle_status,
  b.winlost_time,
  b.pull_time,
  b.currency,
  b.game_id,
  b.device,
  b.odds_type,
  b.odds,
  b.is_combo
FROM
  bc_bet_records_3027 b
WHERE
  bet_time >= ?
  AND bet_time <= ?
  AND site_code = ?
  AND currency = ?
ORDER BY
  bet_time DESC
LIMIT
  ?, ?`

	// findBetRecordsListBySettleTimeSQL filters by category_id and settle_time.
	findBetRecordsListBySettleTimeSQL = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
SELECT
  /*+ read_from_storage(tiflash[b]) */
  b.record_id,
  b.order_no,
  b.round_id,
  b.account,
  b.third_user_name,
  b.third_game_code,
  b.site_code,
  b.platform_id,
  b.category_id gameCategoryId,
  b.bet_time,
  b.settle_time,
  b.all_bet,
  b.valid_bet,
  b.net_profit,
  b.after_balance,
  b.tax,
  b.rake,
  b.insurance,
  b.props,
  b.settle_status,
  b.winlost_time,
  b.pull_time,
  b.currency,
  b.game_id,
  b.device,
  b.odds_type,
  b.odds,
  b.is_combo
FROM
  bc_bet_records_280 b
WHERE
  category_id IN (?)
  AND settle_time >= ?
  AND settle_time <= ?
  AND site_code = ?
  AND currency = ?
ORDER BY
  settle_time DESC
LIMIT
  ?, ?`

	defaultRewriter = &Rewriter{
		digestAllowlist: newDigestAllowlist(
			findBetRecordsListSQL,
			findBetRecordsListBySettleTimeSQL,
		),
	}
)

func newDigestAllowlist(sqls ...string) map[string]struct{} {
	allowlist := make(map[string]struct{}, len(sqls))
	for _, sql := range sqls {
		allowlist[ReplayDigest(sql)] = struct{}{}
	}
	return allowlist
}

// Rewriter rewrites SQL statements before replay execution.
type Rewriter struct {
	digestAllowlist map[string]struct{}
}

// DefaultRewriter returns the built-in rewriter for known SQL patterns.
func DefaultRewriter() *Rewriter {
	return defaultRewriter
}

// ReplayDigest computes the logical digest used for matching shard-table SQL variants.
func ReplayDigest(sql string) string {
	sql = tiflashReadHintRE.ReplaceAllString(sql, "")
	sql = shardTableRE.ReplaceAllString(sql, "bc_bet_records")
	_, digest := parser.NormalizeDigest(sql)
	return digest.String()
}

// StripTiflashReadHint removes optimizer hints like /*+ read_from_storage(tiflash[b]) */.
func StripTiflashReadHint(sql string) string {
	return tiflashReadHintRE.ReplaceAllString(sql, "")
}

// PrependIgnorePlanCacheBeforeTiflashHint inserts /*+ ignore_plan_cache() */ immediately before the tiflash read hint.
func PrependIgnorePlanCacheBeforeTiflashHint(sql string) (string, bool) {
	if !tiflashReadHintRE.MatchString(sql) || ignorePlanCacheRE.MatchString(sql) {
		return sql, false
	}
	loc := tiflashReadHintRE.FindStringIndex(sql)
	if loc == nil {
		return sql, false
	}
	return sql[:loc[0]] + ignorePlanCacheHint + " " + sql[loc[0]:], true
}

// MaybeRewrite rewrites SQL before replay execution.
// For allowlisted digests, it strips the tiflash read hint.
// For other SQL with a tiflash read hint, it prepends /*+ ignore_plan_cache() */.
func (r *Rewriter) MaybeRewrite(sql string) (string, bool) {
	if r == nil || !tiflashReadHintRE.MatchString(sql) {
		return sql, false
	}
	if len(r.digestAllowlist) > 0 {
		if _, ok := r.digestAllowlist[ReplayDigest(sql)]; ok {
			return StripTiflashReadHint(sql), true
		}
	}
	return PrependIgnorePlanCacheBeforeTiflashHint(sql)
}

// RewriteCommand rewrites the SQL text carried by command before sending it to TiDB.
func (r *Rewriter) RewriteCommand(command *cmd.Command) bool {
	if r == nil || command == nil {
		return false
	}
	switch command.Type {
	case pnet.ComQuery, pnet.ComStmtPrepare:
		sql := hack.String(command.Payload[1:])
		newSQL, ok := r.MaybeRewrite(sql)
		if !ok {
			return false
		}
		command.Payload = append([]byte{command.Type.Byte()}, hack.Slice(newSQL)...)
		return true
	case pnet.ComStmtExecute, pnet.ComStmtFetch, pnet.ComStmtClose, pnet.ComStmtReset, pnet.ComStmtSendLongData:
		if command.PreparedStmt == "" {
			return false
		}
		newSQL, ok := r.MaybeRewrite(command.PreparedStmt)
		if !ok {
			return false
		}
		command.PreparedStmt = newSQL
		return true
	default:
		return false
	}
}
