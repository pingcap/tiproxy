// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import "time"

const (
	fileNamePrefix     = "traffic-"
	fileNameSuffix     = ".log"
	fileCompressFormat = ".gz"
	fileSize           = 300 << 20
	bufferSize         = 1 << 20
	opTimeout          = 10 * time.Second
)
