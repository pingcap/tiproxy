// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package vip

import (
	"net"

	"github.com/pingcap/tiproxy/lib/util/errors"
)

func arpSendGratuitous(_ string, _ net.IP) error {
	return errors.New("ARP is not supported on this platform")
}
