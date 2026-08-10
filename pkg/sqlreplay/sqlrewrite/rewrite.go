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
	betRecordForceIndexRE   = regexp.MustCompile(`(?i)(\bbc_bet_records_\d+)\s+(\w+)`)
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

	sql20 = `
  /* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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

	sql21 = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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
  AND bet_time >= ?
  AND bet_time <= ?
  AND site_code = ?
ORDER BY
  bet_time DESC
LIMIT
  ?, ?`

	sql22 = `/* SQL_TAG(BcBetRecordsMapper.sumBetRecordAmountForGo) */
SELECT
  count(1) AS total,
  SUM(all_bet) AS total_all_bet,
  SUM(valid_bet) AS total_valid_bet,
  SUM(net_profit) AS total_net_profit,
  SUM(tax) AS total_tax
FROM
  bc_bet_records_1150 b FORCE INDEX(idx_account_category_settime)
WHERE
  account = ?
  AND category_id = ?
  AND settle_status = ?
  AND (
    (
      settle_time >= ?
      AND settle_time <= ?
    )
    OR settle_time = '2100-01-01 00:00:00'
  )
	AND site_code = ?`

	sql23 = `/* SQL_TAG(BcBetPromotionMapper.sumTotalBetByDateTime) */
SELECT
  SUM(all_bet) AS total_all_bet,
  SUM(valid_bet) AS total_valid_bet,
  SUM(net_profit) AS total_net_profit
FROM
  bc_bet_records_280 bru FORCE INDEX(idx_account_settletime)
WHERE
  (
    (
      platform_id = ?
    )
  )
  AND account IN (?)
  AND settle_status = ?
  AND settle_time >= ?
  AND settle_time <= ?
  AND site_code = ?`

	sql24 = `/* SQL_TAG(BcBetRecordsMapper.sumBetRecordAmountForGo) */
SELECT
  count(1) AS total,
  SUM(all_bet) AS total_all_bet,
  SUM(valid_bet) AS total_valid_bet,
  SUM(net_profit) AS total_net_profit,
  SUM(tax) AS total_tax
FROM
  bc_bet_records_3209 b FORCE INDEX(idx_account_category_settime)
WHERE
  account = ?
  AND category_id = ?
  AND settle_status IN (1, 2, 3)
  AND (
    (
      settle_time >= ?
      AND settle_time <= ?
    )
    OR settle_time = '2100-01-01 00:00:00'
  )
	AND site_code = ?`

	sql25 = `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsForGoCustomPage) */
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
  bc_bet_records_1316 b FORCE INDEX(idx_account_category_settime)
WHERE
  account = ?
  AND category_id = ?
  AND settle_status = ?
  AND (
    (
      settle_time >= ?
      AND settle_time <= ?
    )
    OR settle_time = '2100-01-01 00:00:00'
  )
  AND site_code = ?
ORDER BY
  settle_time DESC
LIMIT
  ?, ?`

	defaultRewriter = &Rewriter{
		digestAllowlist: newDigestAllowlist(
			sql1, sql2, sql3, sql5, sql6, sql7, sql8,
		),
		forceIndexDigestAllowlist:                         newDigestAllowlist(sql9),
		betRecordSumForceIndexDigestAllowlist:             newDigestAllowlist(sql10, sql12),
		betRecordListForceIndexDigestAllowlist:            newDigestAllowlist(sql11, sql18, sql20),
		betRecordCategoryForceIndexDigestAllowlist:        newDigestAllowlist(sql21),
		betRecordPlatformSettimeForceIndexDigestAllowlist: newDigestAllowlist(sql23),
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
	lg                                                *zap.Logger
	digestAllowlist                                   map[string]struct{}
	forceIndexDigestAllowlist                         map[string]struct{}
	betRecordSumForceIndexDigestAllowlist             map[string]struct{}
	betRecordListForceIndexDigestAllowlist            map[string]struct{}
	betRecordCategoryForceIndexDigestAllowlist        map[string]struct{}
	betRecordPlatformSettimeForceIndexDigestAllowlist map[string]struct{}
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

const betRecordCategoryBetTimeIndexName = "idx_category_id_bet_time"

// AddBetRecordCategoryForceIndex adds FORCE INDEX(idx_category_id_bet_time) to matching shard tables.
func AddBetRecordCategoryForceIndex(sql string) (string, bool) {
	if strings.Contains(strings.ToUpper(sql), "FORCE INDEX") {
		return sql, false
	}
	if !betRecordForceIndexRE.MatchString(sql) {
		return sql, false
	}
	newSQL := betRecordForceIndexRE.ReplaceAllString(sql, `${1} ${2} FORCE INDEX(`+betRecordCategoryBetTimeIndexName+`)`)
	return newSQL, newSQL != sql
}

// StripTiflashAndAddCategoryForceIndex strips the tiflash read hint and adds FORCE INDEX(idx_category_id_bet_time).
func StripTiflashAndAddCategoryForceIndex(sql string) (string, bool) {
	newSQL := StripTiflashReadHint(sql)
	forced, added := AddBetRecordCategoryForceIndex(newSQL)
	if added {
		return forced, true
	}
	return newSQL, newSQL != sql
}

const (
	betRecordAccountBettimeIndexName                       = "idx_account_bettime"
	betRecordAccountBetTimeCoverIndexName                  = "idx_account_bet_time_cover"
	betRecordAccountSettletimeIndexName                    = "idx_account_settletime"
	betRecordAccountPlatformSettimeIndexName               = "idx_account_platform_settime"
	betRecordAccountSettleTimeStatusCategoryCoverIndexName = "idx_account_settle_time_status_category_cover"
)

var (
	accountBettimeIndexRE         = regexp.MustCompile(`(?i)idx_account_bettime`)
	accountSettletimeIndexRE      = regexp.MustCompile(`(?i)idx_account_settletime`)
	accountCategorySettimeIndexRE = regexp.MustCompile(`(?i)idx_account_category_settime`)
)

// ReplaceAllAccountBettimeIndex replaces every idx_account_bettime with idx_account_bet_time_cover.
func ReplaceAllAccountBettimeIndex(sql string) (string, bool) {
	if !accountBettimeIndexRE.MatchString(sql) {
		return sql, false
	}
	return accountBettimeIndexRE.ReplaceAllString(sql, betRecordAccountBetTimeCoverIndexName), true
}

// ReplaceAllAccountSettletimeIndex replaces every idx_account_settletime with idx_account_settle_time_status_category_cover.
func ReplaceAllAccountSettletimeIndex(sql string) (string, bool) {
	if !accountSettletimeIndexRE.MatchString(sql) {
		return sql, false
	}
	return accountSettletimeIndexRE.ReplaceAllString(sql, betRecordAccountSettleTimeStatusCategoryCoverIndexName), true
}

// ReplaceAllAccountCategorySettimeIndex replaces every idx_account_category_settime with idx_account_settle_time_status_category_cover.
func ReplaceAllAccountCategorySettimeIndex(sql string) (string, bool) {
	if !accountCategorySettimeIndexRE.MatchString(sql) {
		return sql, false
	}
	return accountCategorySettimeIndexRE.ReplaceAllString(sql, betRecordAccountSettleTimeStatusCategoryCoverIndexName), true
}

// ReplaceAccountPlatformSettimeForceIndex replaces FORCE INDEX(idx_account_settletime)
// with FORCE INDEX(idx_account_platform_settime).
func ReplaceAccountPlatformSettimeForceIndex(sql string) (string, bool) {
	return replaceForceIndexName(sql, betRecordAccountSettletimeIndexName, betRecordAccountPlatformSettimeIndexName)
}

// replaceAccountIndexNames applies all global account index renames.
func replaceAccountIndexNames(sql string) (string, bool) {
	changed := false
	if newSQL, ok := ReplaceAllAccountBettimeIndex(sql); ok {
		sql = newSQL
		changed = true
	}
	if newSQL, ok := ReplaceAllAccountSettletimeIndex(sql); ok {
		sql = newSQL
		changed = true
	}
	if newSQL, ok := ReplaceAllAccountCategorySettimeIndex(sql); ok {
		sql = newSQL
		changed = true
	}
	return sql, changed
}

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

// ReplaceBetRecordSumForceIndex replaces FORCE INDEX(idx_account_bettime) with FORCE INDEX(idx_account_bet_time_cover).
func ReplaceBetRecordSumForceIndex(sql string) (string, bool) {
	return replaceForceIndexName(sql, betRecordAccountBettimeIndexName, betRecordAccountBetTimeCoverIndexName)
}

// ReplaceBetRecordListForceIndex replaces FORCE INDEX(idx_account_bettime) with FORCE INDEX(idx_account_bet_time_cover).
func ReplaceBetRecordListForceIndex(sql string) (string, bool) {
	return replaceForceIndexName(sql, betRecordAccountBettimeIndexName, betRecordAccountBetTimeCoverIndexName)
}

// MaybeRewrite rewrites SQL before replay execution.
// It replaces every idx_account_bettime with idx_account_bet_time_cover.
// It replaces every idx_account_settletime with idx_account_settle_time_status_category_cover,
// except allowlisted digests on sumTotalBetByDateTime which use idx_account_platform_settime instead.
// It replaces every idx_account_category_settime with idx_account_settle_time_status_category_cover.
// For allowlisted digests on game summary queries, it adds FORCE INDEX (idx_gameid_settleday).
// For allowlisted digests on category+bet_time list queries, it strips tiflash hints and adds FORCE INDEX(idx_category_id_bet_time).
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
	// Previously only allowlisted sum queries replaced idx_account_bettime with idx_account_bet_time_cover.
	// Now all SQL does that replacement above; keep the allowlisted SQL samples but disable this path.
	// if len(r.betRecordSumForceIndexDigestAllowlist) > 0 {
	// 	if _, ok := r.betRecordSumForceIndexDigestAllowlist[ReplayDigest(sql)]; ok {
	// 		replacedSql, replaced := ReplaceBetRecordSumForceIndex(sql)
	// 		if ReplayDigest(sql) == ReplayDigest(sql12) && r.lg != nil {
	// 			if !replaced {
	// 				r.lg.Info("match sql12", zap.Bool("replaced", replaced), zap.String("sql", sql))
	// 			} else {
	// 				r.lg.Info("match sql12", zap.Bool("replaced", replaced))
	// 			}
	// 		}
	// 		return replacedSql, replaced
	// 	}
	// }
	if len(r.betRecordListForceIndexDigestAllowlist) > 0 {
		if _, ok := r.betRecordListForceIndexDigestAllowlist[ReplayDigest(sql)]; ok {
			return ReplaceBetRecordListForceIndex(sql)
		}
	}
	// Higher priority than the global idx_account_settletime rewrite below.
	if len(r.betRecordPlatformSettimeForceIndexDigestAllowlist) > 0 {
		if _, ok := r.betRecordPlatformSettimeForceIndexDigestAllowlist[ReplayDigest(sql)]; ok {
			return ReplaceAccountPlatformSettimeForceIndex(sql)
		}
	}
	if newSQL, ok := replaceAccountIndexNames(sql); ok {
		return newSQL, true
	}
	if len(r.betRecordCategoryForceIndexDigestAllowlist) > 0 {
		if _, ok := r.betRecordCategoryForceIndexDigestAllowlist[ReplayDigest(sql)]; ok {
			return StripTiflashAndAddCategoryForceIndex(sql)
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
