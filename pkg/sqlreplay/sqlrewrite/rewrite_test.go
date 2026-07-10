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
	digest3027 := ReplayDigest(findBetRecordsListSQL)
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

func TestReplayDigestIgnoresHintAndShardSuffixForSettleTimeQuery(t *testing.T) {
	digest280 := ReplayDigest(findBetRecordsListBySettleTimeSQL)
	digest3027 := ReplayDigest(`/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */
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
  category_id IN (?)
  AND settle_time >= ?
  AND settle_time <= ?
  AND site_code = ?
  AND currency = ?
ORDER BY
  settle_time DESC
LIMIT
  ?, ?`)

	require.Equal(t, digest280, digest3027)
	require.Contains(t, defaultRewriter.digestAllowlist, digest280)
}

func TestDefaultRewriterSettleTimeQuery(t *testing.T) {
	rewriter := DefaultRewriter()
	rewritten, ok := rewriter.MaybeRewrite(findBetRecordsListBySettleTimeSQL)
	require.True(t, ok)
	require.NotContains(t, rewritten, "read_from_storage")
	require.NotContains(t, rewritten, "ignore_plan_cache")
}

func TestStripTiflashReadHintKeepsSQLTag(t *testing.T) {
	stripped := StripTiflashReadHint(findBetRecordsListSQL)
	require.Contains(t, stripped, "/* SQL_TAG(BcBetRecordsMapper.findBetRecordsList) */")
	require.NotContains(t, stripped, "read_from_storage")
	require.Contains(t, stripped, "bc_bet_records_3027")
}

func TestPrependIgnorePlanCacheBeforeTiflashHint(t *testing.T) {
	sql := "SELECT /*+ read_from_storage(tiflash[b]) */ 1"
	rewritten, ok := PrependIgnorePlanCacheBeforeTiflashHint(sql)
	require.True(t, ok)
	require.Equal(t, "SELECT /*+ ignore_plan_cache() */ /*+ read_from_storage(tiflash[b]) */ 1", rewritten)

	_, ok = PrependIgnorePlanCacheBeforeTiflashHint(rewritten)
	require.False(t, ok)

	_, ok = PrependIgnorePlanCacheBeforeTiflashHint("SELECT 1")
	require.False(t, ok)
}

func TestDefaultRewriter(t *testing.T) {
	rewriter := DefaultRewriter()
	require.NotNil(t, rewriter)

	_, ok := rewriter.MaybeRewrite("SELECT 1")
	require.False(t, ok)

	_, ok = rewriter.MaybeRewrite(findBetRecordsListSQL)
	require.True(t, ok)

	_, ok = rewriter.MaybeRewrite(findBetRecordsListBySettleTimeSQL)
	require.True(t, ok)

	rewritten, ok := rewriter.MaybeRewrite("SELECT /*+ read_from_storage(tiflash[t]) */ 1")
	require.True(t, ok)
	require.Contains(t, rewritten, "ignore_plan_cache")
	require.Contains(t, rewritten, "read_from_storage")
}

func TestRewriteCommandComQuery(t *testing.T) {
	rewriter := DefaultRewriter()
	command := &cmd.Command{
		Type:    pnet.ComQuery,
		Payload: append([]byte{pnet.ComQuery.Byte()}, []byte(findBetRecordsListSQL)...),
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
		Payload: append([]byte{pnet.ComStmtPrepare.Byte()}, []byte(findBetRecordsListSQL)...),
	}
	require.True(t, rewriter.RewriteCommand(command))
	require.NotContains(t, string(command.Payload[1:]), "read_from_storage")
	require.NotContains(t, string(command.Payload[1:]), "ignore_plan_cache")
}

func TestRewriteCommandComStmtExecute(t *testing.T) {
	rewriter := DefaultRewriter()
	command := &cmd.Command{
		Type:         pnet.ComStmtExecute,
		PreparedStmt: findBetRecordsListSQL,
		Payload:      []byte{pnet.ComStmtExecute.Byte(), 1, 0, 0, 0},
	}
	require.True(t, rewriter.RewriteCommand(command))
	require.NotContains(t, command.PreparedStmt, "read_from_storage")
	require.NotContains(t, command.PreparedStmt, "ignore_plan_cache")
	require.Contains(t, command.PreparedStmt, "SQL_TAG")
}
