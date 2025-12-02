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

func NetAddr2IP(addr net.Addr) (net.IP, error) {
	if addr == nil || reflect.ValueOf(addr).IsNil() {
		return nil, errors.New("address is nil")
	}
	value := addr.String()
	ipStr, _, err := net.SplitHostPort(value)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse address '%s'", value)
	}
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return nil, errors.Errorf("failed to parse IP '%s'", value)
	}
	return ip, nil
}

func CIDRContainsIP(cidrList []*net.IPNet, ip net.IP) (bool, error) {
	if ip == nil || reflect.ValueOf(ip).IsNil() {
		return false, errors.New("address is nil")
	}
	for _, cidr := range cidrList {
		if cidr.Contains(ip) {
			return true, nil
		}
	}
	return false, nil
}

func IsPrivate(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	if ipv4 := ip.To4(); ipv4 != nil {
		if ipv4[0] == 100 && ipv4[1] >= 64 && ipv4[1] <= 127 {
			return true
		}
	}
	return ip.IsPrivate()
}
