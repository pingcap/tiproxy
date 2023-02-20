// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"strconv"
	"time"

	"github.com/pingcap/TiProxy/pkg/metrics"
	"github.com/pingcap/tidb/parser/mysql"
)

// The labels are consistent with TiDB.
var (
	cmdToLabel = map[byte]string{
		mysql.ComSleep:            "Sleep",
		mysql.ComQuit:             "Quit",
		mysql.ComInitDB:           "InitDB",
		mysql.ComQuery:            "Query",
		mysql.ComPing:             "Ping",
		mysql.ComFieldList:        "FieldList",
		mysql.ComStmtPrepare:      "StmtPrepare",
		mysql.ComStmtExecute:      "StmtExecute",
		mysql.ComStmtFetch:        "StmtFetch",
		mysql.ComStmtClose:        "StmtClose",
		mysql.ComStmtSendLongData: "StmtSendLongData",
		mysql.ComStmtReset:        "StmtReset",
		mysql.ComSetOption:        "SetOption",
	}
)

func addCmdMetrics(cmd byte, addr string, startTime time.Time) {
	label, ok := cmdToLabel[cmd]
	if !ok {
		label = strconv.Itoa(int(cmd))
	}
	metrics.QueryTotalCounter.WithLabelValues(addr, label).Inc()

	// The duration labels are different with TiDB: Labels in TiDB are statement types.
	// However, the proxy is not aware of the statement types, so we use command types instead.
	cost := time.Since(startTime)
	metrics.QueryDurationHistogram.WithLabelValues(addr, label).Observe(cost.Seconds())
}

func readCmdCounter(cmd byte, addr string) (int, error) {
	label, ok := cmdToLabel[cmd]
	if !ok {
		label = strconv.Itoa(int(cmd))
	}
	return metrics.ReadCounter(metrics.QueryTotalCounter.WithLabelValues(addr, label))
}

func addGetBackendMetrics(duration time.Duration) {
	metrics.GetBackendHistogram.Observe(float64(duration.Milliseconds()))
}
