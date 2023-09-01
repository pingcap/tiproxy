// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package proxyprotocol

import "github.com/pingcap/tiproxy/lib/util/errors"

var (
	ErrAddressFamilyMismatch = errors.New("address family between source and target mismatched")
)
