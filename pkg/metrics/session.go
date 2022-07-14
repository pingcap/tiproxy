// Copyright 2020 Ipalfish, Inc.
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

package metrics

// Label constants.
const (
	LblUnretryable = "unretryable"
	LblReachMax    = "reach_max"
	LblOK          = "ok"
	LblError       = "error"
	LblCommit      = "commit"
	LblAbort       = "abort"
	LblRollback    = "rollback"
	LblType        = "type"
	LblDb          = "db"
	LblTable       = "table"
	LblResult      = "result"
	LblSQLType     = "sql_type"
	LblGeneral     = "general"
	LblInternal    = "internal"
	LbTxnMode      = "txn_mode"
	LblPessimistic = "pessimistic"
	LblOptimistic  = "optimistic"
	LblStore       = "store"
	LblAddress     = "address"
	LblBatchGet    = "batch_get"
	LblGet         = "get"
	LblNamespace   = "namespace"
	LblCluster     = "cluster"

	LblBackendAddr = "backend_addr"
)
