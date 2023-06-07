// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package client

import "github.com/pingcap/TiProxy/lib/util/errors"

var (
	ErrCloseConn = errors.New("failed to close client connection")
)
