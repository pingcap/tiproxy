// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

type Balance struct {
	Error ErrorBalance `yaml:"error,omitempty" toml:"error,omitempty" json:"error,omitempty"`
	Label LabelBalance `yaml:"label,omitempty" toml:"label,omitempty" json:"label,omitempty"`
}

type ErrorBalance struct {
	Indicators []ErrorIndicator `yaml:"indicators,omitempty" toml:"indicators,omitempty" json:"indicators,omitempty"`
}

type ErrorIndicator struct {
	QueryExpr        string `yaml:"query-expr,omitempty" toml:"query-expr,omitempty" json:"query-expr,omitempty"`
	FailThreshold    int    `yaml:"fail-threshold,omitempty" toml:"fail-threshold,omitempty" json:"fail-threshold,omitempty"`
	RecoverThreshold int    `yaml:"recover-threshold,omitempty" toml:"recover-threshold,omitempty" json:"recover-threshold,omitempty"`
}

type LabelBalance struct {
	LabelName string `yaml:"label-name,omitempty" toml:"label-name,omitempty" json:"label-name,omitempty"`
}

// NewDefaultErrorBalance initializes default error indicators.
//
// The chosen metrics must meet some requirements:
//  1. To treat a backend as normal, all the metrics should be normal.
//     E.g. tidb_session_schema_lease_error_total is always 0 even if the backend doesn't recover when it has no connection,
//     so we need other metrics to judge whether the backend recovers.
//  2. Unstable (not only unavailable) network should also be treated as abnormal.
//     E.g. Renewing lease may succeed sometimes and `time() - tidb_domain_lease_expire_time` may look normal
//     even when the network is unstable, so we need other metrics to judge unstable network.
//  3. `failThreshold - recoverThreshold` should be big enough so that TiProxy won't mistakenly migrate connections.
//     E.g. If TiKV is unavailable, all backends may report the same errors. We can ensure the error is caused by this TiDB
//     only when other TiDB report much less errors.
//  4. The metric value of a normal backend with high CPS should be less than `failThreshold` and the value of an abnormal backend
//     with 0 CPS should be greater than `recoverThreshold`.
//     E.g. tidb_tikvclient_backoff_seconds_count may be high when CPS is high on a normal backend, and may be very low
//     when CPS is 0 on an abnormal backend.
//  5. Normal metrics must keep for some time before treating the backend as normal to avoid frequent migration.
//     E.g. Unstable network may lead to repeated fluctuations of error counts.
func DefaultErrorBalance() ErrorBalance {
	return ErrorBalance{
		Indicators: []ErrorIndicator{
			{
				// may be caused by disconnection to PD
				// test with no connection: around 80
				QueryExpr:        `sum(increase(tidb_tikvclient_backoff_seconds_count{type="pdRPC"}[1m])) by (instance)`,
				FailThreshold:    100,
				RecoverThreshold: 10,
			},
			{
				// may be caused by disconnection to TiKV
				// test with no connection: regionMiss is around 1300, tikvRPC is around 40
				QueryExpr:        `sum(increase(tidb_tikvclient_backoff_seconds_count{type=~"regionMiss|tikvRPC"}[1m])) by (instance)`,
				FailThreshold:    1000,
				RecoverThreshold: 100,
			},
		},
	}
}
