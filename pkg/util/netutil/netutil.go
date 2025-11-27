// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package netutil

import (
	"net"
	"reflect"
	"strings"

	"github.com/pingcap/tiproxy/lib/util/errors"
)

func ParseCIDRList(strList []string) ([]*net.IPNet, error) {
	cidrList := make([]*net.IPNet, 0, len(strList))
	var parseErr error
	for _, v := range strList {
		if !strings.Contains(v, "/") {
			v = v + "/32"
		}
		_, cidr, err := net.ParseCIDR(v)
		if err == nil {
			cidrList = append(cidrList, cidr)
		} else {
			parseErr = err
		}
	}
	return cidrList, parseErr
}

func CIDRContainsIP(cidrList []*net.IPNet, addr net.Addr) (bool, error) {
	if addr == nil || reflect.ValueOf(addr).IsNil() {
		return false, errors.New("address is nil")
	}
	value := addr.String()
	ipStr, _, err := net.SplitHostPort(value)
	if err != nil {
		return false, errors.Wrapf(err, "failed to parse address '%s'", value)
	}
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false, errors.Errorf("failed to parse IP '%s'", value)
	}
	for _, cidr := range cidrList {
		if cidr.Contains(ip) {
			return true, nil
		}
	}
	return false, nil
}
