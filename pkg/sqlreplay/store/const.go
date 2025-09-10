// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import "time"

const (
	fileNamePrefix      = "traffic-"
	auditFileNamePrefix = "tidb-audit-"
	fileNameSuffix      = ".log"
	logTimeLayout       = "2006-01-02T15-04-05.999"
	fileCompressFormat  = ".gz"
	fileSize            = 300 << 20
	bufferSize          = 1 << 20
	opTimeout           = 10 * time.Second
)
