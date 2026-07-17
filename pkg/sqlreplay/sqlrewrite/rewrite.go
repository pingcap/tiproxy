// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package sqlrewrite

import (
	"regexp"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/siddontang/go/hack"
	"go.uber.org/zap"
)

var (
	tiflashReadHintRE       = regexp.MustCompile(`(?is)/\*\s*\+\s*(read_from_storage\s*\(\s*tiflash\s*\[\s*b\s*\]\s*\))\s*\*/`)
	ignorePlanCacheRE       = regexp.MustCompile(`(?is)ignore_plan_cache\s*\(\s*\)`)
	shardTableRE            = regexp.MustCompile(`(?i)\bbc_bet_records_\d+\b`)
	gameSummaryTableRE      = regexp.MustCompile(`(?i)\bbc_order_account_game_summary_\d+\b`)
	gameSummaryForceIndexRE = regexp.MustCompile(`(?i)(\bbc_order_account_game_summary_\d+)\s+(\w+)`)
	sql1                    = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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

	sql2 = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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

	sql3 = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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
  bc_bet_records_3030 b
WHERE
  category_id IN (?)
  AND bet_time >= ?
  AND bet_time <= ?
  AND site_code = ?
  AND currency = ?
ORDER BY
  bet_time DESC
LIMIT
  ?, ?`

	sql4 = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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
  AND all_bet >= ?
ORDER BY
  settle_time DESC
LIMIT
  ?, ?`

	sql5 = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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
  AND all_bet >= ?
ORDER BY
  net_profit DESC,
  id DESC
LIMIT
  ?, ?`

	sql6 = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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
  bc_bet_records_1226 b
WHERE
  platform_id = ?
  AND bet_time >= ?
  AND bet_time <= ?
  AND site_code = ?
  AND currency = ?
ORDER BY
  bet_time DESC
LIMIT
  ?, ?`

	sql7 = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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
  bc_bet_records_3238 b
WHERE
  settle_time >= ?
  AND settle_time <= ?
  AND site_code = ?
  AND currency = ?
ORDER BY
  settle_time DESC
LIMIT
  ?, ?`

	sql8 = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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
  bc_bet_records_669 b
WHERE
  bet_time >= ?
  AND bet_time <= ?
  AND site_code = ?
ORDER BY
  bet_time DESC
LIMIT
  ?, ?`

	sql9 = `/* SQL_TAG(BcOrderAccountGameSummaryMapper.getJackPotPool) */
SELECT
  site_code,
  game_category_id,
  game_platform_id,
  currency,
  sum(profit_total) profit_total
FROM
  bc_order_account_game_summary t
WHERE
  game_id < 0
  AND settle_day >= ?
  AND settle_day <= ?
  AND site_code IN (?)
  AND currency IS NOT NULL
  AND currency != ''
GROUP BY
  site_code,
  game_category_id,
  game_platform_id,
  currency`

	sql10 = `/* SQL_TAG(BcBetRecordsMapper.sumBetRecordAmount) */
SELECT
  count(1) AS total,
  SUM(all_bet) AS total_all_bet,
  SUM(valid_bet) AS total_valid_bet,
  SUM(net_profit) AS total_net_profit,
  SUM(tax) AS total_tax,
  SUM(rake) AS total_rake,
  SUM(insurance) AS total_insurance,
  SUM(props) AS total_props
FROM
  bc_bet_records_2798 b FORCE INDEX(idx_account_bettime)
WHERE
  account = ?
  AND bet_time >= ?
  AND bet_time <= ?
  AND site_code = ?
  AND currency = ?`

	sql11 = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
  SELECT
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
    bc_bet_records_3050 b FORCE INDEX(idx_account_bettime)
  WHERE
    account = ?
    AND bet_time >= ?
    AND bet_time <= ?
    AND site_code = ?
    AND currency = ?
  ORDER BY
    net_profit DESC,
    id DESC
  LIMIT
    ?, ?`

	sql12 = `/* SQL_TAG(BcBetRecordsMapper.sumBetRecordAmount) */
SELECT
  count(1) AS total
FROM
  bc_bet_records_169 b FORCE INDEX(idx_account_bettime)
WHERE
  account = ?
  AND bet_time >= ?
  AND bet_time <= ?
  AND site_code = ?`

	sql13 = `/* SQL_TAG(BcBetRecordsMapper.sumBetRecordAmount) */
SELECT
  /*+ read_from_storage(tiflash[b]) */
  count(1) AS total,
  SUM(all_bet) AS total_all_bet,
  SUM(valid_bet) AS total_valid_bet,
  SUM(net_profit) AS total_net_profit,
  SUM(tax) AS total_tax,
  SUM(rake) AS total_rake,
  SUM(insurance) AS total_insurance,
  SUM(props) AS total_props
FROM
  bc_bet_records_280 b
WHERE
  category_id IN (?)
  AND settle_time >= ?
  AND settle_time <= ?
  AND site_code = ?
  AND currency = ?
  AND all_bet >= ?`

	sql14 = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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
  AND all_bet >= ?
ORDER BY
  settle_time DESC
LIMIT
  ?, ?`

	sql15 = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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
  bc_bet_records_2868 b
WHERE
  game_id IN (
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?
  )
  AND category_id IN (?)
  AND settle_time >= ?
  AND settle_time <= ?
  AND site_code = ?
  AND currency = ?
  AND all_bet >= ?
ORDER BY
  settle_time DESC
LIMIT
  ?, ?`

	sql16 = `/* SQL_TAG(BcBetRecordsMapper.sumBetRecordAmount) */
SELECT
  /*+ read_from_storage(tiflash[b]) */
  count(1) AS total,
  SUM(all_bet) AS total_all_bet,
  SUM(valid_bet) AS total_valid_bet,
  SUM(net_profit) AS total_net_profit,
  SUM(tax) AS total_tax,
  SUM(rake) AS total_rake,
  SUM(insurance) AS total_insurance,
  SUM(props) AS total_props
FROM
  bc_bet_records_2868 b
WHERE
  category_id IN (?)
  AND settle_time >= ?
  AND settle_time <= ?
  AND site_code = ?
  AND currency = ?
  AND all_bet >= ?`

	sql17 = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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
  AND all_bet >= ?
ORDER BY
  settle_time DESC
LIMIT
  ?, ?`

	sql18 = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
SELECT
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
  bc_bet_records_878 b FORCE INDEX(idx_account_bettime)
WHERE
  account = ?
  AND bet_time >= ?
  AND bet_time <= ?
  AND site_code = ?
  AND currency = ?
ORDER BY
  net_profit ASC,
  id
LIMIT
  ?, ?`

	sql19 = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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
  AND platform_id = ?
  AND bet_time >= ?
  AND bet_time <= ?
  AND site_code = ?
  AND currency = ?
  AND all_bet >= ?
  AND all_bet <= ?
ORDER BY
  bet_time DESC
LIMIT
  ?, ?`

	defaultRewriter = &Rewriter{
		digestAllowlist: newDigestAllowlist(
			sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8, sql13, sql14, sql15, sql16, sql17, sql19,
		),
		forceIndexDigestAllowlist:              newDigestAllowlist(sql9),
		betRecordSumForceIndexDigestAllowlist:  newDigestAllowlist(sql10, sql12),
		betRecordListForceIndexDigestAllowlist: newDigestAllowlist(sql11, sql18),
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
	lg                                     *zap.Logger
	digestAllowlist                        map[string]struct{}
	forceIndexDigestAllowlist              map[string]struct{}
	betRecordSumForceIndexDigestAllowlist  map[string]struct{}
	betRecordListForceIndexDigestAllowlist map[string]struct{}
}

// DefaultRewriter returns the built-in rewriter for known SQL patterns.
func DefaultRewriter(lg *zap.Logger) *Rewriter {
	defaultRewriter.lg = lg
	return defaultRewriter
}

// ReplayDigest computes the logical digest used for matching shard-table SQL variants.
func ReplayDigest(sql string) string {
	sql = tiflashReadHintRE.ReplaceAllString(sql, "")
	sql = shardTableRE.ReplaceAllString(sql, "bc_bet_records")
	sql = gameSummaryTableRE.ReplaceAllString(sql, "bc_order_account_game_summary")
	_, digest := parser.NormalizeDigest(sql)
	return digest.String()
}

// StripTiflashReadHint removes optimizer hints like /*+ read_from_storage(tiflash[b]) */.
func StripTiflashReadHint(sql string) string {
	return tiflashReadHintRE.ReplaceAllString(sql, "")
}

// PrependIgnorePlanCacheBeforeTiflashHint merges ignore_plan_cache into the tiflash read hint comment.
func PrependIgnorePlanCacheBeforeTiflashHint(sql string) (string, bool) {
	if !tiflashReadHintRE.MatchString(sql) || ignorePlanCacheRE.MatchString(sql) {
		return sql, false
	}
	newSQL := tiflashReadHintRE.ReplaceAllString(sql, "/*+ ignore_plan_cache() ${1} */")
	return newSQL, true
}

// AddGameSummaryForceIndex adds FORCE INDEX (idx_gameid_settleday) to matching shard tables.
func AddGameSummaryForceIndex(sql string) (string, bool) {
	if strings.Contains(strings.ToUpper(sql), "FORCE INDEX") {
		return sql, false
	}
	if !gameSummaryForceIndexRE.MatchString(sql) {
		return sql, false
	}
	newSQL := gameSummaryForceIndexRE.ReplaceAllString(sql, `${1} ${2} FORCE INDEX (idx_gameid_settleday)`)
	return newSQL, newSQL != sql
}

const betRecordAccountBettimeIndexName = "idx_account_bettime"

// replaceForceIndexName replaces an index name using case-insensitive string matching.
func replaceForceIndexName(sql, fromIndex, toIndex string) (string, bool) {
	lowerSQL := strings.ToLower(sql)
	lowerFrom := strings.ToLower(fromIndex)
	lowerTo := strings.ToLower(toIndex)
	if strings.Contains(lowerSQL, lowerTo) {
		return sql, false
	}
	start := strings.Index(lowerSQL, lowerFrom)
	if start < 0 {
		return sql, false
	}
	end := start + len(fromIndex)
	return sql[:start] + toIndex + sql[end:], true
}

// ReplaceBetRecordSumForceIndex replaces FORCE INDEX(idx_account_bettime) with FORCE INDEX(idx_account_sum_bet_amount).
func ReplaceBetRecordSumForceIndex(sql string) (string, bool) {
	return replaceForceIndexName(sql, betRecordAccountBettimeIndexName, "idx_account_sum_bet_amount")
}

// ReplaceBetRecordListForceIndex replaces FORCE INDEX(idx_account_bettime) with FORCE INDEX(idx_catagory_account_bet_time_net_profit).
func ReplaceBetRecordListForceIndex(sql string) (string, bool) {
	return replaceForceIndexName(sql, betRecordAccountBettimeIndexName, "idx_catagory_account_bet_time_net_profit")
}

// MaybeRewrite rewrites SQL before replay execution.
// For allowlisted digests on bet record list queries, it replaces FORCE INDEX(idx_account_bettime).
// For allowlisted digests on bet record sum queries, it replaces FORCE INDEX(idx_account_bettime).
// For allowlisted digests on game summary queries, it adds FORCE INDEX (idx_gameid_settleday).
// For allowlisted digests with a tiflash read hint, it strips the tiflash read hint.
// For other SQL with a tiflash read hint, it merges /*+ ignore_plan_cache() */ into the same hint comment.
func (r *Rewriter) MaybeRewrite(sql string) (string, bool) {
	if r == nil {
		return sql, false
	}
	if len(r.forceIndexDigestAllowlist) > 0 {
		if _, ok := r.forceIndexDigestAllowlist[ReplayDigest(sql)]; ok {
			return AddGameSummaryForceIndex(sql)
		}
	}
	if len(r.betRecordSumForceIndexDigestAllowlist) > 0 {
		if _, ok := r.betRecordSumForceIndexDigestAllowlist[ReplayDigest(sql)]; ok {
			replacedSql, replaced := ReplaceBetRecordSumForceIndex(sql)
			if ReplayDigest(sql) == ReplayDigest(sql12) && r.lg != nil {
				if !replaced {
					r.lg.Info("match sql12", zap.Bool("replaced", replaced), zap.String("sql", sql))
				} else {
					r.lg.Info("match sql12", zap.Bool("replaced", replaced))
				}
			}
			return replacedSql, replaced
		}
	}
	if len(r.betRecordListForceIndexDigestAllowlist) > 0 {
		if _, ok := r.betRecordListForceIndexDigestAllowlist[ReplayDigest(sql)]; ok {
			return ReplaceBetRecordListForceIndex(sql)
		}
	}
	if !tiflashReadHintRE.MatchString(sql) {
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
