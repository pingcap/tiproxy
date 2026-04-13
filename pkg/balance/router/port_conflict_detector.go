// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import "github.com/pingcap/tiproxy/lib/util/errors"

type portConflictDetector struct {
	routes  map[string]*Group
	blocked map[string]error
	owners  map[string]string
}

func newPortConflictDetector() *portConflictDetector {
	return &portConflictDetector{
		routes:  make(map[string]*Group),
		blocked: make(map[string]error),
		owners:  make(map[string]string),
	}
}

func (v *portConflictDetector) bind(port, clusterName string, group *Group) {
	if port == "" {
		return
	}
	if _, blocked := v.blocked[port]; blocked {
		return
	}
	if owner, ok := v.owners[port]; !ok {
		v.owners[port] = clusterName
		v.routes[port] = group
		return
	} else if owner != clusterName {
		v.blocked[port] = errors.Wrapf(ErrPortConflict, "listener port %s is claimed by multiple backend clusters", port)
		delete(v.routes, port)
		return
	}
	v.routes[port] = group
}

func (v *portConflictDetector) groupFor(port string) (*Group, error) {
	if port == "" {
		return nil, nil
	}
	if err, ok := v.blocked[port]; ok {
		return nil, err
	}
	return v.routes[port], nil
}
