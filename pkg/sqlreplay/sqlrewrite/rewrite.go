// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

// Package sqlrewrite rewrites SQL statements before replay execution.
//
// SQL-to-digest mappings are stored in digest_mapping.local.md in this directory (local file, not committed).
// Do not add test files when modifying rewrite.go.
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
)

const (
	digest1  = "dfd04c40ccaaab8609437ee6e37b7509a65f72fde4e5f97728b8af4a99365ee5"
	digest2  = "97632fd51b060b85c180bf04e151ce6e70ad9581d28982b9f13673bc2098f3af"
	digest3  = "1b9479e97b784bfcace552566c77055aa64210aac37f3bc67f4544dd17fc4883"
	digest5  = "d582212a208e1028238c40319f24d095a271eb64706fce48142c141449833ed8"
	digest6  = "e514d68795cf8c874f8f532c589cb94e082aaae922a1a276c917421c8485dcad"
	digest7  = "96eb81fdedb4e77ff9e36a7de21ab0e30110e99dff2086c525fb18620f256832"
	digest8  = "67b2ed88bbf05ef9eab0c3657c481487217bb491ea9107e76fdd1f3d70676e73"
	digest9  = "578d0b725ef6ffbd966b889275c980c850ad8cde06934c2e44c0dabd79aa21bd"
	digest10 = "65061f7ddbda2cd113e8457d54f5b4890d112bee2ab63ad9ef714ad5f75c0ee5"
	digest11 = "ecde82e73ea947a76e1c4f7e756615b575c7430739aaac6ea7c1ab7f4e44e540"
	digest12 = "dce4e4910c87b2ab1f3b00a9d911d158d7e18be43b510ee3350d4e1d1d2e0ff5"
	digest18 = "ec91ab8947937c2883bab57e529f8d9fe38f66ca18dc061ccaf3a9c23f6636ce"
	digest21 = "d89dfac1dcb5938ab130998b3da1897d16150a9ddf008faff76a1c0bd7ba7029"
)

var (
	defaultRewriter = &Rewriter{
		digestAllowlist: newDigestSet(
			digest1, digest2, digest3, digest5, digest6, digest7, digest8,
		),
		forceIndexDigestAllowlist:                  newDigestSet(digest9),
		betRecordSumForceIndexDigestAllowlist:      newDigestSet(digest10, digest12),
		betRecordListForceIndexDigestAllowlist:     newDigestSet(digest11, digest18),
		betRecordCategoryForceIndexDigestAllowlist: newDigestSet(digest21),
	}
)

func newDigestSet(digests ...string) map[string]struct{} {
	allowlist := make(map[string]struct{}, len(digests))
	for _, digest := range digests {
		allowlist[digest] = struct{}{}
	}
	return allowlist
}

// Rewriter rewrites SQL statements before replay execution.
type Rewriter struct {
	lg                                         *zap.Logger
	digestAllowlist                            map[string]struct{}
	forceIndexDigestAllowlist                  map[string]struct{}
	betRecordSumForceIndexDigestAllowlist      map[string]struct{}
	betRecordListForceIndexDigestAllowlist     map[string]struct{}
	betRecordCategoryForceIndexDigestAllowlist map[string]struct{}
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
	betRecordAccountSettleTimeStatusCategoryCoverIndexName = "idx_account_settle_time_status_category_cover_new"
)

var (
	accountBettimeIndexRE         = regexp.MustCompile(`(?i)idx_account_bettime`)
	accountSettletimeIndexRE      = regexp.MustCompile(`(?i)idx_account_settletime`)
	accountCategorySettimeIndexRE = regexp.MustCompile(`(?i)idx_account_category_settime`)
	accountPlatformSettimeIndexRE = regexp.MustCompile(`(?i)idx_account_platform_settime`)
	accountGidSettimeIndexRE      = regexp.MustCompile(`(?i)idx_account_gid_settletime`)
)

// ReplaceAllAccountBettimeIndex replaces every idx_account_bettime with idx_account_bet_time_cover.
func ReplaceAllAccountBettimeIndex(sql string) (string, bool) {
	if !accountBettimeIndexRE.MatchString(sql) {
		return sql, false
	}
	return accountBettimeIndexRE.ReplaceAllString(sql, betRecordAccountBetTimeCoverIndexName), true
}

// ReplaceAllAccountSettletimeIndex replaces every idx_account_settletime with idx_account_settle_time_status_category_cover_new.
func ReplaceAllAccountSettletimeIndex(sql string) (string, bool) {
	if !accountSettletimeIndexRE.MatchString(sql) {
		return sql, false
	}
	return accountSettletimeIndexRE.ReplaceAllString(sql, betRecordAccountSettleTimeStatusCategoryCoverIndexName), true
}

// ReplaceAllAccountCategorySettimeIndex replaces every idx_account_category_settime with idx_account_settle_time_status_category_cover_new.
func ReplaceAllAccountCategorySettimeIndex(sql string) (string, bool) {
	if !accountCategorySettimeIndexRE.MatchString(sql) {
		return sql, false
	}
	return accountCategorySettimeIndexRE.ReplaceAllString(sql, betRecordAccountSettleTimeStatusCategoryCoverIndexName), true
}

// ReplaceAllAccountPlatformSettimeIndex replaces every idx_account_platform_settime with idx_account_settle_time_status_category_cover_new.
func ReplaceAllAccountPlatformSettimeIndex(sql string) (string, bool) {
	if !accountPlatformSettimeIndexRE.MatchString(sql) {
		return sql, false
	}
	return accountPlatformSettimeIndexRE.ReplaceAllString(sql, betRecordAccountSettleTimeStatusCategoryCoverIndexName), true
}

// ReplaceAllAccountGidSettimeIndex replaces every idx_account_gid_settletime with idx_account_settle_time_status_category_cover_new.
func ReplaceAllAccountGidSettimeIndex(sql string) (string, bool) {
	if !accountGidSettimeIndexRE.MatchString(sql) {
		return sql, false
	}
	return accountGidSettimeIndexRE.ReplaceAllString(sql, betRecordAccountSettleTimeStatusCategoryCoverIndexName), true
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
	if newSQL, ok := ReplaceAllAccountPlatformSettimeIndex(sql); ok {
		sql = newSQL
		changed = true
	}
	if newSQL, ok := ReplaceAllAccountGidSettimeIndex(sql); ok {
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
// It replaces every idx_account_settletime with idx_account_settle_time_status_category_cover_new.
// It replaces every idx_account_category_settime with idx_account_settle_time_status_category_cover_new.
// It replaces every idx_account_platform_settime with idx_account_settle_time_status_category_cover_new.
// It replaces every idx_account_gid_settletime with idx_account_settle_time_status_category_cover_new.
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
	// 		if ReplayDigest(sql) == digest12 && r.lg != nil {
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
