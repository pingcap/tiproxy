// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package sqlrewrite

import (
	"testing"

	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/stretchr/testify/require"
)

func TestReplayDigestIgnoresHintAndShardSuffix(t *testing.T) {
	digest3027 := ReplayDigest(sql1)
	digest1073 := ReplayDigest(`/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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
  bc_bet_records_1073 b
WHERE
  bet_time >= ?
  AND bet_time <= ?
  AND site_code = ?
  AND currency = ?
ORDER BY
  bet_time DESC
LIMIT
  ?, ?`)
	digestNoHint := ReplayDigest(`/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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
  bc_bet_records_3027 b
WHERE
  bet_time >= ?
  AND bet_time <= ?
  AND site_code = ?
  AND currency = ?
ORDER BY
  bet_time DESC
LIMIT
  ?, ?`)

	require.Equal(t, digest3027, digest1073)
	require.Equal(t, digest3027, digestNoHint)
	require.Contains(t, defaultRewriter.digestAllowlist, digest3027)
}

func TestReplayDigestIgnoresGameSummaryShardSuffix(t *testing.T) {
	digestBase := ReplayDigest(sql9)
	digest1161 := ReplayDigest(`/* SQL_TAG(BcOrderAccountGameSummaryMapper.getJackPotPool) */
SELECT
  site_code,
  game_category_id,
  game_platform_id,
  currency,
  sum(profit_total) profit_total
FROM
  bc_order_account_game_summary_1161 t
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
  currency`)

	require.Equal(t, digestBase, digest1161)
	require.Contains(t, defaultRewriter.forceIndexDigestAllowlist, digestBase)
}

func TestMaybeRewriteAddsGameSummaryForceIndex(t *testing.T) {
	sql := `/* SQL_TAG(BcOrderAccountGameSummaryMapper.getJackPotPool) */
SELECT
  site_code,
  game_category_id,
  game_platform_id,
  currency,
  sum(profit_total) profit_total
FROM
  bc_order_account_game_summary_1161 t
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

	rewriter := DefaultRewriter()
	newSQL, ok := rewriter.MaybeRewrite(sql)
	require.True(t, ok)
	require.Contains(t, newSQL, "bc_order_account_game_summary_1161 t FORCE INDEX (idx_gameid_settleday)")
}

func TestRewriteCommandGameSummaryComQuery(t *testing.T) {
	sql := `/* SQL_TAG(BcOrderAccountGameSummaryMapper.getJackPotPool) */
SELECT
  site_code,
  game_category_id,
  game_platform_id,
  currency,
  sum(profit_total) profit_total
FROM
  bc_order_account_game_summary_1161 t
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

	rewriter := DefaultRewriter()
	command := &cmd.Command{
		Type:    pnet.ComQuery,
		Payload: append([]byte{pnet.ComQuery.Byte()}, []byte(sql)...),
	}
	require.True(t, rewriter.RewriteCommand(command))
	require.Contains(t, string(command.Payload[1:]), "FORCE INDEX (idx_gameid_settleday)")
}

func TestReplayDigestIgnoresBetRecordSumShardSuffix(t *testing.T) {
	digestBase := ReplayDigest(sql10)
	digest1073 := ReplayDigest(`/* SQL_TAG(BcBetRecordsMapper.sumBetRecordAmount) */
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
  bc_bet_records_1073 b FORCE INDEX(idx_account_bettime)
WHERE
  account = ?
  AND bet_time >= ?
  AND bet_time <= ?
  AND site_code = ?
  AND currency = ?`)

	require.Equal(t, digestBase, digest1073)
	require.Contains(t, defaultRewriter.betRecordSumForceIndexDigestAllowlist, digestBase)
}

func TestReplayDigestIgnoresBetRecordSumCountOnlyShardSuffix(t *testing.T) {
	digestBase := ReplayDigest(sql12)
	digest1073 := ReplayDigest(`/* SQL_TAG(BcBetRecordsMapper.sumBetRecordAmount) */
SELECT
  count(1) AS total
FROM
  bc_bet_records_1073 b FORCE INDEX(idx_account_bettime)
WHERE
  account = ?
  AND bet_time >= ?
  AND bet_time <= ?
  AND site_code = ?`)

	require.Equal(t, digestBase, digest1073)
	require.Contains(t, defaultRewriter.betRecordSumForceIndexDigestAllowlist, digestBase)
}

func TestReplayDigestIgnoresCombinedTiflashHintForSumBetRecordAmount(t *testing.T) {
	digestBase := ReplayDigest(sql13)
	digest1073 := ReplayDigest(`/* SQL_TAG(BcBetRecordsMapper.sumBetRecordAmount) */
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
  bc_bet_records_1073 b
WHERE
  category_id IN (?)
  AND settle_time >= ?
  AND settle_time <= ?
  AND site_code = ?
  AND currency = ?
  AND all_bet >= ?`)
	digestNoHint := ReplayDigest(`/* SQL_TAG(BcBetRecordsMapper.sumBetRecordAmount) */
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
  bc_bet_records_280 b
WHERE
  category_id IN (?)
  AND settle_time >= ?
  AND settle_time <= ?
  AND site_code = ?
  AND currency = ?
  AND all_bet >= ?`)

	require.Equal(t, digestBase, digest1073)
	require.Equal(t, digestBase, digestNoHint)
	require.Contains(t, defaultRewriter.digestAllowlist, digestBase)
}

func TestMaybeRewriteStripsCombinedTiflashHintForSumBetRecordAmount(t *testing.T) {
	rewriter := DefaultRewriter()
	rewritten, ok := rewriter.MaybeRewrite(sql13)
	require.True(t, ok)
	require.NotContains(t, rewritten, "read_from_storage")
	require.NotContains(t, rewritten, "ignore_plan_cache")
	require.Contains(t, rewritten, "bc_bet_records_280")
}

func TestMaybeRewriteReplacesBetRecordSumForceIndex(t *testing.T) {
	sql := `/* SQL_TAG(BcBetRecordsMapper.sumBetRecordAmount) */
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

	rewriter := DefaultRewriter()
	newSQL, ok := rewriter.MaybeRewrite(sql)
	require.True(t, ok)
	require.Contains(t, newSQL, "FORCE INDEX(idx_account_sum_bet_amount)")
	require.NotContains(t, newSQL, "idx_account_bettime")
}

func TestRewriteCommandBetRecordSumComQuery(t *testing.T) {
	sql := `/* SQL_TAG(BcBetRecordsMapper.sumBetRecordAmount) */
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

	rewriter := DefaultRewriter()
	command := &cmd.Command{
		Type:    pnet.ComQuery,
		Payload: append([]byte{pnet.ComQuery.Byte()}, []byte(sql)...),
	}
	require.True(t, rewriter.RewriteCommand(command))
	require.Contains(t, string(command.Payload[1:]), "FORCE INDEX(idx_account_sum_bet_amount)")
	require.NotContains(t, string(command.Payload[1:]), "idx_account_bettime")
}

func TestReplayDigestIgnoresBetRecordListForceIndexShardSuffix(t *testing.T) {
	digestBase := ReplayDigest(sql11)
	digest1073 := ReplayDigest(`/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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
  bc_bet_records_1073 b FORCE INDEX(idx_account_bettime)
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
  ?, ?`)

	require.Equal(t, digestBase, digest1073)
	require.Contains(t, defaultRewriter.betRecordListForceIndexDigestAllowlist, digestBase)
}

func TestMaybeRewriteReplacesBetRecordListForceIndex(t *testing.T) {
	sql := `/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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

	rewriter := DefaultRewriter()
	newSQL, ok := rewriter.MaybeRewrite(sql)
	require.True(t, ok)
	require.Contains(t, newSQL, "bc_bet_records_3050")
	require.Contains(t, newSQL, "FORCE INDEX(idx_catagory_account_bet_time_net_profit)")
	require.NotContains(t, newSQL, "idx_account_bettime")
}

func TestMaybeRewriteReplacesBetRecordListForceIndexAscOrder(t *testing.T) {
	rewriter := DefaultRewriter()
	newSQL, ok := rewriter.MaybeRewrite(sql18)
	require.True(t, ok)
	require.Contains(t, newSQL, "bc_bet_records_878")
	require.Contains(t, newSQL, "FORCE INDEX(idx_catagory_account_bet_time_net_profit)")
	require.NotContains(t, newSQL, "idx_account_bettime")
	require.Contains(t, newSQL, "net_profit ASC")
}

func TestRewriteCommandComQuery(t *testing.T) {
	rewriter := DefaultRewriter()
	command := &cmd.Command{
		Type:    pnet.ComQuery,
		Payload: append([]byte{pnet.ComQuery.Byte()}, []byte(sql1)...),
	}
	require.True(t, rewriter.RewriteCommand(command))
	require.NotContains(t, string(command.Payload[1:]), "read_from_storage")
	require.NotContains(t, string(command.Payload[1:]), "ignore_plan_cache")
	require.Contains(t, string(command.Payload[1:]), "SQL_TAG")
}

func TestRewriteCommandComStmtPrepare(t *testing.T) {
	rewriter := DefaultRewriter()
	command := &cmd.Command{
		Type:    pnet.ComStmtPrepare,
		Payload: append([]byte{pnet.ComStmtPrepare.Byte()}, []byte(sql1)...),
	}
	require.True(t, rewriter.RewriteCommand(command))
	require.NotContains(t, string(command.Payload[1:]), "read_from_storage")
	require.NotContains(t, string(command.Payload[1:]), "ignore_plan_cache")
}

func TestRewriteCommandComStmtExecute(t *testing.T) {
	rewriter := DefaultRewriter()
	command := &cmd.Command{
		Type:         pnet.ComStmtExecute,
		PreparedStmt: sql1,
		Payload:      []byte{pnet.ComStmtExecute.Byte(), 1, 0, 0, 0},
	}
	require.True(t, rewriter.RewriteCommand(command))
	require.NotContains(t, command.PreparedStmt, "read_from_storage")
	require.NotContains(t, command.PreparedStmt, "ignore_plan_cache")
	require.Contains(t, command.PreparedStmt, "SQL_TAG")
}
