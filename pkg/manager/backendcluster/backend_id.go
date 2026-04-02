// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backendcluster

import (
	"fmt"
	"strings"
)

// backendID returns the opaque identity key for one backend in one backend cluster.
// It is only used as an in-memory map key and must not be parsed or used as a network address.
func backendID(clusterName, addr string) string {
	return fmt.Sprintf("%s/%s", clusterName, addr)
}

// ParseBackendID parses the backendID into clusterName and addr.
// Please avoid using this function as much as possible, and only use it when you cannot find
// any other way to get the clusterName and addr. The backendID is designed to be opaque and should not
// be parsed in most cases.
func ParseBackendID(id string) (clusterName, addr string) {
	parts := strings.Split(id, "/")
	if len(parts) >= 2 {
		return strings.Join(parts[:len(parts)-1], "/"), parts[len(parts)-1]
	} else if len(parts) == 1 {
		return "", parts[0]
	}

	// This branch is indeed impossible
	return "", ""
}
