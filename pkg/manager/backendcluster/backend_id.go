// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backendcluster

import "fmt"

// backendID returns the opaque identity key for one backend in one backend cluster.
// It is only used as an in-memory map key and must not be parsed or used as a network address.
func backendID(clusterName, addr string) string {
	return fmt.Sprintf("%s/%s", clusterName, addr)
}
