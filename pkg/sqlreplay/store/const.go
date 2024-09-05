// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

const (
	fileNamePrefix     = "traffic"
	fileNameSuffix     = ".log"
	fileName           = fileNamePrefix + fileNameSuffix
	fileTsLayout       = "2006-01-02T15-04-05.000"
	fileCompressFormat = ".gz"
	fileSize           = 300 // 300MB
)
