// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package sys

import "net"

func GetGlobalUnicastIP() string {
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, address := range addrs {
			ipnet, ok := address.(*net.IPNet)
			if ok && ipnet.IP.IsGlobalUnicast() {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
